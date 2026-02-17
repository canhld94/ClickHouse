#!/usr/bin/env node
/**
 * Fetch ClickHouse CI test reports without Playwright
 *
 * Usage:
 *   node fetch_ci_report.js <url> [options]
 *
 * Options:
 *   --test <name>    Filter to show only tests matching this name
 *   --failed         Show only failed tests
 *   --all            Show all test results (not just summary)
 *   --links          Show artifact links
 *   --download-logs  Download logs.tar.gz to /tmp/ci_logs.tar.gz
 *   --credentials <user,password>  HTTP Basic Auth credentials (comma-separated)
 *
 * Examples:
 *   node fetch_ci_report.js "https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=94537&..."
 *   node fetch_ci_report.js "<url>" --test peak_memory --links
 *   node fetch_ci_report.js "<url>" --failed --download-logs
 */

const https = require('https');
const http = require('http');
const { URL } = require('url');
const fs = require('fs');
const { execSync } = require('child_process');

/**
 * Normalize task name as done in the HTML page
 */
function normalizeTaskName(name) {
  return name.toLowerCase()
    .replace(/[^a-z0-9]/g, '_')
    .replace(/_+/g, '_')
    .replace(/_+$/, '');
}

/**
 * Fetch a URL and return the response body
 */
function fetchUrl(urlString, credentials = null) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(urlString);
    const protocol = parsedUrl.protocol === 'https:' ? https : http;

    const options = {
      method: 'GET',
      headers: {}
    };

    if (credentials) {
      const auth = Buffer.from(`${credentials.username}:${credentials.password}`).toString('base64');
      options.headers['Authorization'] = `Basic ${auth}`;
    }

    const req = protocol.get(urlString, options, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        // Follow redirect
        return fetchUrl(res.headers.location, credentials).then(resolve).catch(reject);
      }

      if (res.statusCode === 403) {
        reject(new Error('403 Forbidden - Report does not exist or expired'));
        return;
      }

      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`));
        return;
      }

      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        resolve(body);
      });
    });

    req.on('error', reject);
    req.setTimeout(60000, () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
  });
}

/**
 * Parse the HTML URL to extract parameters and construct JSON URLs
 */
function parseReportUrl(htmlUrl) {
  const url = new URL(htmlUrl);
  const params = url.searchParams;

  const PR = params.get('PR');
  const REF = params.get('REF');
  const sha = params.get('sha');
  const base_url = params.get('base_url');

  // Extract name parameters (name_0, name_1, etc.)
  const nameParams = [];
  params.forEach((value, key) => {
    if (key.startsWith('name_')) {
      const index = parseInt(key.split('_')[1], 10);
      nameParams[index] = value;
    }
  });

  // Construct base URL
  let baseUrl = base_url;
  if (!baseUrl) {
    // Default to the S3 bucket path
    baseUrl = url.origin + url.pathname.split('/').slice(0, -1).join('/');
  }

  // Construct suffix
  let suffix = '';
  if (PR) {
    suffix = `PRs/${encodeURIComponent(PR)}`;
  } else if (REF) {
    suffix = `REFs/${encodeURIComponent(REF)}`;
  } else {
    throw new Error('Either PR or REF parameter is required');
  }

  if (!sha) {
    throw new Error('sha parameter is required');
  }

  if (nameParams.length === 0) {
    throw new Error('At least name_0 parameter is required');
  }

  return { baseUrl, suffix, sha, nameParams };
}

/**
 * Construct JSON URL for a given task name
 */
function constructJsonUrl(baseUrl, suffix, sha, taskName) {
  const normalizedTask = normalizeTaskName(taskName);
  return `${baseUrl}/${suffix}/${encodeURIComponent(sha)}/result_${normalizedTask}.json`;
}

/**
 * Parse test results from the JSON data
 */
function parseTestResults(jsonData) {
  const tests = [];

  if (!jsonData || !jsonData.results) {
    return tests;
  }

  function extractTests(results, prefix = '') {
    for (const result of results) {
      if (result.results && result.results.length > 0) {
        // Nested results
        extractTests(result.results, prefix ? `${prefix}/${result.name}` : result.name);
      } else {
        // Leaf result - this is a test
        tests.push({
          name: prefix ? `${prefix}/${result.name}` : result.name,
          status: result.status || 'UNKNOWN',
          duration: result.duration || 0
        });
      }
    }
  }

  extractTests(jsonData.results);
  return tests;
}

/**
 * Extract artifact links from JSON data
 */
function extractArtifactLinks(jsonData) {
  const links = [];

  if (!jsonData) {
    return links;
  }

  // Extract links from the top-level links array
  if (jsonData.links) {
    for (const link of jsonData.links) {
      if (typeof link === 'string') {
        links.push({ text: link.split('/').pop(), href: link });
      }
    }
  }

  // Extract links from results
  function extractFromResults(results) {
    if (!results) return;

    for (const result of results) {
      if (result.links) {
        for (const link of result.links) {
          if (typeof link === 'string') {
            links.push({ text: link.split('/').pop(), href: link });
          }
        }
      }
      if (result.results) {
        extractFromResults(result.results);
      }
    }
  }

  extractFromResults(jsonData.results);

  // Filter to only artifact links
  return links.filter(link =>
    link.href.includes('.tar.gz') ||
    link.href.includes('.log') ||
    link.href.includes('configs')
  );
}

/**
 * Fetch and parse the CI report
 */
async function fetchReport(htmlUrl, options = {}) {
  try {
    console.log(`Parsing URL: ${htmlUrl}\n`);

    // Parse the URL to get parameters
    const { baseUrl, suffix, sha, nameParams } = parseReportUrl(htmlUrl);

    console.log(`Task: ${nameParams.join(' -> ')}`);
    console.log(`SHA: ${sha}\n`);

    // Construct JSON URL for the primary task (name_0)
    const jsonUrl = constructJsonUrl(baseUrl, suffix, sha, nameParams[0]);
    console.log(`Fetching JSON: ${jsonUrl}\n`);

    // Fetch the JSON data
    const jsonText = await fetchUrl(jsonUrl, options.credentials);
    const jsonData = JSON.parse(jsonText);

    // If there are nested tasks (name_1), navigate to them
    let targetData = jsonData;
    if (nameParams.length > 1 && jsonData.results) {
      for (let i = 1; i < nameParams.length; i++) {
        const taskName = nameParams[i];
        targetData = jsonData.results.find(r => r.name === taskName);
        if (!targetData) {
          throw new Error(`Task not found: ${taskName}`);
        }
      }
    }

    // Parse test results
    const testResults = parseTestResults(targetData);

    // Extract artifact links
    const artifactLinks = extractArtifactLinks(jsonData);

    // Apply filters
    let filteredResults = testResults;

    if (options.testFilter) {
      filteredResults = filteredResults.filter(t =>
        t.name.toLowerCase().includes(options.testFilter.toLowerCase())
      );
    }

    if (options.failedOnly) {
      filteredResults = filteredResults.filter(t =>
        t.status === 'failed' || t.status === 'FAIL'
      );
    }

    // Print results
    console.log('=== Test Results ===\n');

    const failed = filteredResults.filter(t => t.status === 'failed' || t.status === 'FAIL');
    const passed = filteredResults.filter(t => t.status === 'success' || t.status === 'OK');
    const skipped = filteredResults.filter(t => t.status === 'skipped' || t.status === 'SKIPPED');

    console.log(`Total: ${filteredResults.length} | Passed: ${passed.length} | Failed: ${failed.length} | Skipped: ${skipped.length}\n`);

    if (failed.length > 0) {
      console.log('--- Failed Tests ---');
      for (const test of failed) {
        console.log(`FAIL  ${test.name}  (${test.duration}s)`);
      }
      console.log('');
    }

    if (options.showAll && !options.failedOnly) {
      console.log('--- All Tests ---');
      for (const test of filteredResults) {
        const statusLabel = test.status.toUpperCase().padEnd(8);
        console.log(`${statusLabel} ${test.name}  (${test.duration}s)`);
      }
    }

    if (options.showLinks) {
      console.log('\n=== Artifact Links ===');
      if (artifactLinks.length > 0) {
        for (const link of artifactLinks) {
          console.log(`${link.text}: ${link.href}`);
        }
      } else {
        console.log('No artifact links found');
      }
    }

    // Download logs if requested
    if (options.downloadLogs) {
      const logsLink = artifactLinks.find(l => l.href.includes('logs.tar.gz'));
      if (logsLink) {
        console.log(`\nDownloading logs from: ${logsLink.href}`);
        const logsPath = '/tmp/ci_logs.tar.gz';
        execSync(`curl -sL "${logsLink.href}" -o ${logsPath}`);
        console.log(`Logs saved to: ${logsPath}`);

        // List contents
        try {
          console.log('\nLogs archive contents (pytest logs):');
          const contents = execSync(`tar -tzf ${logsPath} | grep -E "pytest.*\\.log$|pytest.*\\.jsonl$" | head -20`).toString();
          console.log(contents || '(no pytest logs found)');
        } catch (e) {
          // Ignore errors from grep/head
        }
      } else {
        console.log('\nNo logs.tar.gz found in artifacts');
      }
    }

    return { testResults: filteredResults, artifactLinks, jsonData };

  } catch (error) {
    console.error(`Error: ${error.message}`);
    process.exit(1);
  }
}

async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0 || args[0] === '--help') {
    console.log(`
Usage: node fetch_ci_report.js <url> [options]

Options:
  --test <name>    Filter to show only tests matching this name
  --failed         Show only failed tests
  --all            Show all test results (not just summary)
  --links          Show artifact links
  --download-logs  Download logs.tar.gz to /tmp/ci_logs.tar.gz
  --credentials <user,password>  HTTP Basic Auth credentials

Examples:
  node fetch_ci_report.js "https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=94537&sha=abc123&name_0=Integration%20tests"
  node fetch_ci_report.js "<url>" --test peak_memory --links
  node fetch_ci_report.js "<url>" --failed --download-logs
  node fetch_ci_report.js "<url>" --credentials "user,password" --failed
`);
    process.exit(0);
  }

  const url = args[0];
  const options = {
    testFilter: null,
    failedOnly: false,
    showAll: false,
    showLinks: false,
    downloadLogs: false,
    credentials: null,
  };

  for (let i = 1; i < args.length; i++) {
    switch (args[i]) {
      case '--test':
        options.testFilter = args[++i];
        break;
      case '--failed':
        options.failedOnly = true;
        break;
      case '--all':
        options.showAll = true;
        break;
      case '--links':
        options.showLinks = true;
        break;
      case '--download-logs':
        options.downloadLogs = true;
        break;
      case '--credentials': {
        const cred = args[++i];
        const commaIdx = cred.indexOf(',');
        if (commaIdx === -1) {
          console.error('Error: --credentials must be in "user,password" format');
          process.exit(1);
        }
        options.credentials = {
          username: cred.substring(0, commaIdx),
          password: cred.substring(commaIdx + 1),
        };
        break;
      }
    }
  }

  await fetchReport(url, options);
}

main().catch(console.error);
