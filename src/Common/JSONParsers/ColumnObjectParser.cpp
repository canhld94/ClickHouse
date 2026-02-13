#include <Common/JSONParsers/ColumnObjectParser.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeArray.h>
#include <Core/Field.h>
#include <Formats/JSONExtractTree.h>

namespace DB
{

bool ColumnObjectParser::Object::find(std::string_view key, Element & result) const
{
    if (object_field.getType() != Field::Types::Object)
        return false;
    
    const auto & obj = object_field.safeGet<DB::Object>();
    auto it = obj.find(String(key));
    if (it != obj.end())
    {
        result = Element(col_object, row, it->second, false);
        return true;
    }
    
    return false;
}

bool ColumnObjectParser::Object::findCaseInsensitive(std::string_view key, Element & result) const
{
    if (object_field.getType() != Field::Types::Object)
        return false;
    
    const auto & obj = object_field.safeGet<DB::Object>();
    
    for (const auto & [obj_key, obj_value] : obj)
    {
        if (obj_key.size() == key.size() && 
            std::equal(obj_key.begin(), obj_key.end(), key.begin(),
                       [](char a, char b) { return tolower(a) == tolower(b); }))
        {
            result = Element(col_object, row, obj_value, false);
            return true;
        }
    }
    
    return false;
}

}  // namespace DB



