SET use_legacy_to_time = 0;

-- Between time/time64 data types, we shouldn't support time zone change as it relates values with date only, so while converting from Time to Time, it shouldn't change
-- We will probably forbid using timezones with Time/Time64 types

-- UTC timezone (result shouldn't change)
SELECT toTime(toTime('000:00:01', 'UTC'), 'Europe/Amsterdam');
SELECT toTime(toTime('00:00:01', 'UTC'), 'Europe/Amsterdam');
SELECT toTime(toTime('0:00:01', 'UTC'), 'Europe/Amsterdam');
SELECT toTime(toTime('-000:00:01', 'UTC'), 'Europe/Amsterdam');
SELECT toTime(toTime('-00:00:01', 'UTC'), 'Europe/Amsterdam');
SELECT toTime(toTime('-0:00:01', 'UTC'), 'Europe/Amsterdam');
SELECT toTime(toTime('-9:59:59', 'UTC'), 'Europe/Amsterdam');
SELECT toTime(toTime('9:59:59', 'UTC'), 'Europe/Amsterdam');
SELECT toTime64(toTime64('000:00:01', 0, 'UTC'), 0, 'Europe/Amsterdam');
SELECT toTime64(toTime64('00:00:01.1', 0, 'UTC'), 0, 'Europe/Amsterdam');
SELECT toTime64(toTime64('0:00:01', 0, 'UTC'), 0, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-000:00:01.1', 0, 'UTC'), 0, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-00:00:01', 0, 'UTC'), 0, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-0:00:01.1', 0, 'UTC'), 0, 'Europe/Amsterdam');
SELECT toTime64(toTime64('001:01:01.1234', 3, 'UTC'), 3, 'Europe/Amsterdam');
SELECT toTime64(toTime64('01:01:01.1234', 3, 'UTC'), 3, 'Europe/Amsterdam');
SELECT toTime64(toTime64('1:01:01.1234', 3, 'UTC'), 3, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-000:00:01.123', 3, 'UTC'), 3, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-00:00:01.123', 3, 'UTC'), 3, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-0:00:01.123', 3, 'UTC'), 3, 'Europe/Amsterdam');
SELECT toTime64(toTime64('000:00:01.123456', 6, 'UTC'), 6, 'Europe/Amsterdam');
SELECT toTime64(toTime64('00:00:01.1234567', 6, 'UTC'), 6, 'Europe/Amsterdam');
SELECT toTime64(toTime64('0:00:01.123456', 6, 'UTC'), 6, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-000:00:01.1234567', 6, 'UTC'), 6, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-00:00:01.123456', 6, 'UTC'), 6, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-0:00:01.1234567', 6, 'UTC'), 6, 'Europe/Amsterdam');
SELECT toTime64(toTime64('000:00:01.1234567', 7, 'UTC'), 7, 'Europe/Amsterdam');
SELECT toTime64(toTime64('00:00:01.12345678', 7, 'UTC'), 7, 'Europe/Amsterdam');
SELECT toTime64(toTime64('0:00:01.1234567', 7, 'UTC'), 7, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-000:00:01.12345678', 7, 'UTC'), 7, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-00:00:01.1234567', 7, 'UTC'), 7, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-0:00:01.12345678', 7, 'UTC'), 7, 'Europe/Amsterdam');
SELECT toTime64(toTime64('000:00:01.1234567891', 9, 'UTC'), 9, 'Europe/Amsterdam');
SELECT toTime64(toTime64('00:00:01.123456789', 9, 'UTC'), 9, 'Europe/Amsterdam');
SELECT toTime64(toTime64('0:00:01.1234567891', 9, 'UTC'), 9, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-000:00:01.123456789', 9, 'UTC'), 9, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-00:00:01.1234567891', 9, 'UTC'), 9, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-0:00:01.123456789', 9, 'UTC'), 9, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-9:99:99', 0, 'UTC'), 0, 'Europe/Amsterdam');
SELECT toTime64(toTime64('9:99:99', 0, 'UTC'), 0, 'Europe/Amsterdam');
SELECT toTime64(toTime64('-9:99:99.1234', 3, 'UTC'), 3, 'Europe/Amsterdam');
SELECT toTime64(toTime64('9:99:99.1234', 3, 'UTC'), 3, 'Europe/Amsterdam');

-- Europe/Amsterdam timezone (should not change as well)
SELECT toTime('000:00:01', 'Europe/Amsterdam');
SELECT toTime('00:00:01', 'Europe/Amsterdam');
SELECT toTime('0:00:01', 'Europe/Amsterdam');
SELECT toTime('-000:00:01', 'Europe/Amsterdam');
SELECT toTime('-00:00:01', 'Europe/Amsterdam');
SELECT toTime('-0:00:01', 'Europe/Amsterdam');
SELECT toTime('-9:59:59', 'Europe/Amsterdam');
SELECT toTime('9:59:59', 'Europe/Amsterdam');
SELECT toTime64('000:00:01', 0, 'Europe/Amsterdam');
SELECT toTime64('00:00:01.1', 0, 'Europe/Amsterdam');
SELECT toTime64('0:00:01', 0, 'Europe/Amsterdam');
SELECT toTime64('-000:00:01.1', 0, 'Europe/Amsterdam');
SELECT toTime64('-00:00:01', 0, 'Europe/Amsterdam');
SELECT toTime64('-0:00:01.1', 0, 'Europe/Amsterdam');
SELECT toTime64('001:01:01.1234', 3, 'Europe/Amsterdam');
SELECT toTime64('01:01:01.1234', 3, 'Europe/Amsterdam');
SELECT toTime64('1:01:01.1234', 3, 'Europe/Amsterdam');
SELECT toTime64('-000:00:01.123', 3, 'Europe/Amsterdam');
SELECT toTime64('-00:00:01.123', 3, 'Europe/Amsterdam');
SELECT toTime64('-0:00:01.123', 3, 'Europe/Amsterdam');
SELECT toTime64('000:00:01.123456', 6, 'Europe/Amsterdam');
SELECT toTime64('00:00:01.1234567', 6, 'Europe/Amsterdam');
SELECT toTime64('0:00:01.123456', 6, 'Europe/Amsterdam');
SELECT toTime64('-000:00:01.1234567', 6, 'Europe/Amsterdam');
SELECT toTime64('-00:00:01.123456', 6, 'Europe/Amsterdam');
SELECT toTime64('-0:00:01.1234567', 6, 'Europe/Amsterdam');
SELECT toTime64('000:00:01.1234567', 7, 'Europe/Amsterdam');
SELECT toTime64('00:00:01.12345678', 7, 'Europe/Amsterdam');
SELECT toTime64('0:00:01.1234567', 7, 'Europe/Amsterdam');
SELECT toTime64('-000:00:01.12345678', 7, 'Europe/Amsterdam');
SELECT toTime64('-00:00:01.1234567', 7, 'Europe/Amsterdam');
SELECT toTime64('-0:00:01.12345678', 7, 'Europe/Amsterdam');
SELECT toTime64('000:00:01.1234567891', 9, 'Europe/Amsterdam');
SELECT toTime64('00:00:01.123456789', 9, 'Europe/Amsterdam');
SELECT toTime64('0:00:01.1234567891', 9, 'Europe/Amsterdam');
SELECT toTime64('-000:00:01.123456789', 9, 'Europe/Amsterdam');
SELECT toTime64('-00:00:01.1234567891', 9, 'Europe/Amsterdam');
SELECT toTime64('-0:00:01.123456789', 9, 'Europe/Amsterdam');
SELECT toTime64('-9:99:99', 0, 'Europe/Amsterdam');
SELECT toTime64('9:99:99', 0, 'Europe/Amsterdam');
SELECT toTime64('-9:99:99.1234', 3, 'Europe/Amsterdam');
SELECT toTime64('9:99:99.1234', 3, 'Europe/Amsterdam');
