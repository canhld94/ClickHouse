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


/// Element implementation

// Type checking methods
bool ColumnObjectParser::Element::isInt64() const
{
    Field field_to_check = field_value;
    if (is_root && col_object)
        field_to_check = (*col_object)[row];
    
    return field_to_check.getType() == Field::Types::Int64;
}

bool ColumnObjectParser::Element::isUInt64() const
{
    Field field_to_check = field_value;
    if (is_root && col_object)
        field_to_check = (*col_object)[row];
    
    return field_to_check.getType() == Field::Types::UInt64;
}

bool ColumnObjectParser::Element::isDouble() const
{
    Field field_to_check = field_value;
    if (is_root && col_object)
        field_to_check = (*col_object)[row];
    
    return field_to_check.getType() == Field::Types::Float64;
}

bool ColumnObjectParser::Element::isBool() const
{
    Field field_to_check = field_value;
    if (is_root && col_object)
        field_to_check = (*col_object)[row];
    
    return field_to_check.getType() == Field::Types::Bool;
}

bool ColumnObjectParser::Element::isString() const
{
    Field field_to_check = field_value;
    if (is_root && col_object)
        field_to_check = (*col_object)[row];
    
    return field_to_check.getType() == Field::Types::String;
}

bool ColumnObjectParser::Element::isArray() const
{
    Field field_to_check = field_value;
    if (is_root && col_object)
        field_to_check = (*col_object)[row];
    
    return field_to_check.getType() == Field::Types::Array;
}

bool ColumnObjectParser::Element::isObject() const
{
    Field field_to_check = field_value;
    if (is_root && col_object)
        field_to_check = (*col_object)[row];
    
    return field_to_check.getType() == Field::Types::Object;
}

bool ColumnObjectParser::Element::isNull() const
{
    Field field_to_check = field_value;
    if (is_root && col_object)
        field_to_check = (*col_object)[row];
    
    return field_to_check.getType() == Field::Types::Null;
}

// Value accessor methods

ElementType ColumnObjectParser::Element::type() const
{
    Field field_to_check = field_value;
    if (is_root && col_object)
        field_to_check = (*col_object)[row];
    switch (field_to_check.getType())
    {
        case Field::Types::Int64:    return ElementType::INT64;
        case Field::Types::UInt64:   return ElementType::UINT64;
        case Field::Types::Float64:  return ElementType::DOUBLE;
        case Field::Types::String:   return ElementType::STRING;
        case Field::Types::Array:    return ElementType::ARRAY;
        case Field::Types::Object:   return ElementType::OBJECT;
        case Field::Types::Bool:     return ElementType::BOOL;
        case Field::Types::Null:     return ElementType::NULL_VALUE;
        default:                     return ElementType::NULL_VALUE;
    }
}
Int64 ColumnObjectParser::Element::getInt64() const
{
    if (is_root && col_object)
    {
        auto field = (*col_object)[row];
        if (field.getType() == Field::Types::Int64)
            return field.safeGet<Int64>();
    }
    else if (field_value.getType() == Field::Types::Int64)
        return field_value.safeGet<Int64>();
    return 0;
}

UInt64 ColumnObjectParser::Element::getUInt64() const
{
    if (is_root && col_object)
    {
        auto field = (*col_object)[row];
        if (field.getType() == Field::Types::UInt64)
            return field.safeGet<UInt64>();
    }
    else if (field_value.getType() == Field::Types::UInt64)
        return field_value.safeGet<UInt64>();
    return 0;
}

double ColumnObjectParser::Element::getDouble() const
{
    if (is_root && col_object)
    {
        auto field = (*col_object)[row];
        if (field.getType() == Field::Types::Float64)
            return field.safeGet<Float64>();
    }
    else if (field_value.getType() == Field::Types::Float64)
        return field_value.safeGet<Float64>();
    return 0.0;
}

bool ColumnObjectParser::Element::getBool() const
{
    if (is_root && col_object)
    {
        auto field = (*col_object)[row];
        if (field.getType() == Field::Types::Bool)
            return field.safeGet<bool>();
    }
    else if (field_value.getType() == Field::Types::Bool)
        return field_value.safeGet<bool>();
    return false;
}

std::string_view ColumnObjectParser::Element::getString() const
{
    if (is_root && col_object)
    {
        auto field = (*col_object)[row];
        if (field.getType() == Field::Types::String)
        {
            const auto & str = field.safeGet<String>();
            return std::string_view(str);
        }
    }
    else if (field_value.getType() == Field::Types::String)
    {
        const auto & str = field_value.safeGet<String>();
        return std::string_view(str);
    }
    return {};
}

ColumnObjectParser::Array ColumnObjectParser::Element::getArray() const
{
    if (is_root && col_object)
    {
        auto field = (*col_object)[row];
        return Array(field, col_object, row);
    }
    return Array(field_value, col_object, row);
}

ColumnObjectParser::Object ColumnObjectParser::Element::getObject() const
{
    if (is_root && col_object)
    {
        auto field = (*col_object)[row];
        return Object(field, col_object, row);
    }
    return Object(field_value, col_object, row);
}

/// Array implementation

ColumnObjectParser::Array::Array(const Field & arr_field_, const ColumnObject * col_obj_, size_t row_)
    : array_field(arr_field_), col_object(col_obj_), row(row_) {}

size_t ColumnObjectParser::Array::size() const
{
    if (array_field.getType() == Field::Types::Array)
    {
        return array_field.safeGet<DB::Array>().size();
    }
    return 0;
}

ColumnObjectParser::Element ColumnObjectParser::Array::operator[](size_t index) const
{
    if (array_field.getType() == Field::Types::Array)
    {
        const auto & arr = array_field.safeGet<DB::Array>();
        if (index < arr.size())
        {
            return Element(col_object, row, arr[index], false);
        }
    }
    return Element();
}

/// Array::Iterator implementation

ColumnObjectParser::Element ColumnObjectParser::Array::Iterator::operator*() const
{
    if (array)
        return (*array)[index];
    return Element();
}

ColumnObjectParser::Array::Iterator & ColumnObjectParser::Array::Iterator::operator++()
{
    ++index;
    return *this;
}

ColumnObjectParser::Array::Iterator ColumnObjectParser::Array::Iterator::operator++(int)
{
    auto tmp = *this;
    ++index;
    return tmp;
}

/// Object implementation

ColumnObjectParser::Object::Object(const Field & obj_field_, const ColumnObject * col_obj_, size_t row_)
    : object_field(obj_field_), col_object(col_obj_), row(row_) {}

size_t ColumnObjectParser::Object::size() const
{
    if (object_field.getType() == Field::Types::Object)
    {
        return object_field.safeGet<DB::Object>().size();
    }
    return 0;
}

bool ColumnObjectParser::Object::find(std::string_view key, Element & result) const
{
    if (object_field.getType() != Field::Types::Object)
        return false;
    
    const auto & obj = object_field.safeGet<DB::Object>();
    String key_str(key);
    
    // Try direct key match first
    auto it = obj.find(key_str);
    if (it != obj.end())
    {
        result = Element(col_object, row, it->second, false);
        return true;
    }
    
    // For ColumnObject with flat paths, try virtual nesting
    // Look for keys that start with "key." to build a virtual nested object
    DB::Object virtual_nested;
    bool found_any = false;
    
    String prefix = String(key) + ".";
    for (const auto & [path, value] : obj)
    {
        if (path.compare(0, prefix.size(), prefix) == 0)
        {
            // Extract the remaining part of the path after the prefix
            String remaining_key = path.substr(prefix.size());
            virtual_nested[remaining_key] = value;
            found_any = true;
        }
    }
    
    if (!found_any)
        return false;
    
    // Create an Element wrapping the virtual nested object
    Field virtual_field(virtual_nested);
    result = Element(col_object, row, virtual_field, false);
    return true;
}

bool ColumnObjectParser::Object::findCaseInsensitive(std::string_view key, Element & result) const
{
    if (object_field.getType() != Field::Types::Object)
        return false;
    
    const auto & obj = object_field.safeGet<DB::Object>();
    
    // Try case-insensitive direct key match
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
    
    // Try case-insensitive virtual nesting for flat paths
    DB::Object virtual_nested;
    bool found_any = false;
    
    for (const auto & [path, value] : obj)
    {
        // Check if path starts with key (case-insensitive) followed by "."
        if (path.size() > key.size() && path[key.size()] == '.')
        {
            if (std::equal(path.begin(), path.begin() + key.size(), key.begin(),
                          [](char a, char b) { return tolower(a) == tolower(b); }))
            {
                String remaining_key = path.substr(key.size() + 1);
                virtual_nested[remaining_key] = value;
                found_any = true;
            }
        }
    }
    
    if (!found_any)
        return false;
    
    Field virtual_field(virtual_nested);
    result = Element(col_object, row, virtual_field, false);
    return true;
}

/// Object::Iterator implementation

ColumnObjectParser::KeyValuePair ColumnObjectParser::Object::Iterator::operator*() const
{
    if (!object)
        return {{}, Element()};
    
    const auto & obj_field = object->object_field;
    if (obj_field.getType() != Field::Types::Object)
        return {{}, Element()};
    
    const auto & obj_map = obj_field.safeGet<DB::Object>();
    
    // Convert path_idx to actual iterator
    // This is inefficient but necessary since we don't store iterators
    auto it = obj_map.begin();
    for (size_t i = 0; i < path_idx && it != obj_map.end(); ++i, ++it)
        ;
    
    if (it == obj_map.end())
        return {{}, Element()};
    
    std::string_view key(it->first);
    Element elem(object->col_object, object->row, it->second, false);
    return {key, elem};
}

ColumnObjectParser::Object::Iterator & ColumnObjectParser::Object::Iterator::operator++()
{
    ++path_idx;
    return *this;
}

ColumnObjectParser::Object::Iterator ColumnObjectParser::Object::Iterator::operator++(int)
{
    auto tmp = *this;
    ++path_idx;
    return tmp;
}

ColumnObjectParser::Object::Iterator ColumnObjectParser::Object::end() const
{
    return Iterator(*this, size());
}

ColumnObjectParser::Object::Iterator::Iterator(const Object & obj_, size_t idx_)
    : object(&obj_), path_idx(idx_) {}

}

