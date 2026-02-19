#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserUnionQueryElement.h>
#include <Common/typeid_cast.h>


namespace DB
{

bool ParserUnionQueryElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (ParserSubquery().parse(pos, node, expected))
    {
        if (const auto * ast_subquery = node->as<ASTSubquery>())
            node = ast_subquery->children.at(0);

        /// ParserSubquery calls ParserSelectWithUnionQuery which always wraps
        /// a single SelectQuery in ASTSelectWithUnionQuery. When this subquery
        /// is used as part of a set operation (e.g. EXCEPT ALL), we need to unwrap
        /// it back to the bare SelectQuery to match the original AST structure.
        if (const auto * union_query = node->as<ASTSelectWithUnionQuery>())
        {
            if (union_query->list_of_selects->children.size() == 1
                && union_query->list_of_selects->children.at(0)->as<ASTSelectQuery>())
            {
                node = union_query->list_of_selects->children.at(0);
            }
        }

        return true;
    }

    if (ParserSelectQuery().parse(pos, node, expected))
        return true;

    return false;
}

}
