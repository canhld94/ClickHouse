#pragma once

#include <base/types.h>
#include <base/defines.h>
#include <Common/JSONParsers/ElementTypes.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeObject.h>
#include <Core/Field.h>
#include <algorithm>
#include <memory>
#include <string_view>

namespace DB
{

/**
 * Parser that reads directly from ColumnObject.
 * Implements the same Element/Array/Object API 
 * operating on ColumnObject's internal structured data.
 */ 
class ColumnObjectParser
{
public:
    class Array;
    class Object;

    /// Represents a JSON value from ColumnObject.
    class Element {
    public:
        Element() = default;

        /// Create element from root of ColumnObject at given row.
        ALWAYS_INLINE Element(const ColumnObject & col_obj_, size_t row_)
            : col_object(&col_obj_), row(row_), is_root(true) {} /// NOLINT

        /// Create element from nested value (internal use).
        ALWAYS_INLINE Element(const ColumnObject * col_obj_, size_t row_, const Field & field_val, bool is_root_ = false)
            : col_object(col_obj_), row(row_), field_value(field_val), is_root(is_root_) {} /// NOLINT

        ALWAYS_INLINE const Field & getField() const
        {
            if (is_root && col_object)
            {
                /// Cache the root field value to avoid returning reference to temporary.
                field_value = (*col_object)[row];
            }
            return field_value;
        }

        ALWAYS_INLINE bool isInt64() const { return getField().getType() == Field::Types::Int64; }
        ALWAYS_INLINE bool isUInt64() const { return getField().getType() == Field::Types::UInt64; }
        ALWAYS_INLINE bool isDouble() const { return getField().getType() == Field::Types::Float64; }
        ALWAYS_INLINE bool isBool() const { return getField().getType() == Field::Types::Bool; }
        ALWAYS_INLINE bool isString() const { return getField().getType() == Field::Types::String; }
        ALWAYS_INLINE bool isArray() const { return getField().getType() == Field::Types::Array; }
        ALWAYS_INLINE bool isObject() const { return getField().getType() == Field::Types::Object; }
        ALWAYS_INLINE bool isNull() const { return getField().getType() == Field::Types::Null; }

        ALWAYS_INLINE Int64 getInt64() const { return getField().safeGet<Int64>(); }
        ALWAYS_INLINE UInt64 getUInt64() const { return getField().safeGet<UInt64>(); }
        ALWAYS_INLINE double getDouble() const { return getField().safeGet<Float64>(); }
        ALWAYS_INLINE bool getBool() const { return getField().safeGet<bool>(); }
        ALWAYS_INLINE std::string_view getString() const { return std::string_view(getField().safeGet<String>()); }
        ALWAYS_INLINE Array getArray() const { return Array(getField(), col_object, row); }
        ALWAYS_INLINE Object getObject() const { return Object(getField(), col_object, row); }

        ALWAYS_INLINE Element getElement() const { return *this; }

        ALWAYS_INLINE ElementType type() const
        {
            const Field & f = getField();
            switch (f.getType())
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

    private:
        const ColumnObject * col_object = nullptr;
        size_t row = 0;
        mutable Field field_value;
        bool is_root = false;

        friend class Array;
        friend class Object;
    };

    /// References an array in JSON.
    class Array
    {
    public:
        class Iterator
        {
        public:
            ALWAYS_INLINE Element operator*() const
            {
                if (array)
                    return (*array)[index];
                return Element();
            }
            
            ALWAYS_INLINE Iterator & operator++()
            {
                ++index;
                return *this;
            }
            
            ALWAYS_INLINE Iterator operator++(int)
            {
                auto tmp = *this;
                ++index;
                return tmp;
            }

            friend bool operator==(const Iterator & a, const Iterator & b)
            {
                return a.index == b.index;
            }
            friend bool operator!=(const Iterator & a, const Iterator & b)
            {
                return a.index != b.index;
            }

        private:
            friend class Array;
            Iterator(const Array & arr_, size_t idx_) : array(&arr_), index(idx_) {}

            const Array * array = nullptr;
            size_t index = 0;
        };

        ALWAYS_INLINE Array(const Field & arr_field_, const ColumnObject * col_obj_, size_t row_)
            : array_field(arr_field_), col_object(col_obj_), row(row_) {} /// NOLINT

        ALWAYS_INLINE Iterator begin() const { return Iterator(*this, 0); }
        ALWAYS_INLINE Iterator end() const { return Iterator(*this, size()); }
        
        ALWAYS_INLINE size_t size() const
        {
            if (array_field.getType() == Field::Types::Array)
                return array_field.safeGet<DB::Array>().size();
            return 0;
        }
        
        ALWAYS_INLINE Element operator[](size_t index) const
        {
            if (array_field.getType() == Field::Types::Array)
            {
                const auto & arr = array_field.safeGet<DB::Array>();
                if (index < arr.size())
                    return Element(col_object, row, arr[index], false);
            }
            return Element();
        }

    private:
        Field array_field;
        const ColumnObject * col_object = nullptr;
        size_t row = 0;

        friend class Iterator;
    };

    using KeyValuePair = std::pair<std::string_view, Element>;

    /// References a JSON object.
    class Object
    {
    public:
        class Iterator
        {
        public:
            ALWAYS_INLINE KeyValuePair operator*() const
            {
                if (!object)
                    return {{}, Element()};
                
                const auto & obj_field = object->object_field;
                if (obj_field.getType() != Field::Types::Object)
                    return {{}, Element()};
                
                const auto & obj_map = obj_field.safeGet<DB::Object>();
                if (current_idx >= obj_map.size())
                    return {{}, Element()};
                
                /// Cache current iterator to avoid O(n) recalculation.
                if (cached_idx != current_idx || !cached_key_ptr)
                {
                    auto it = obj_map.begin();
                    for (size_t i = 0; i < current_idx && it != obj_map.end(); ++i, ++it);
                    
                    if (it != obj_map.end())
                    {
                        cached_idx = current_idx;
                        cached_key_ptr = &it->first;
                        cached_value = it->second;
                    }
                    else
                    {
                        cached_idx = obj_map.size();
                        cached_key_ptr = nullptr;
                    }
                }
                
                if (!cached_key_ptr)
                    return {{}, Element()};
                
                Element elem(object->col_object, object->row, cached_value, false);
                return {std::string_view(*cached_key_ptr), elem};
            }
            
            ALWAYS_INLINE Iterator & operator++()
            {
                ++current_idx;
                cached_key_ptr = nullptr;  // Invalidate cache
                return *this;
            }
            
            ALWAYS_INLINE Iterator operator++(int)
            {
                auto res = *this;
                ++current_idx;
                cached_key_ptr = nullptr;  // Invalidate cache
                return res;
            }

            friend bool operator==(const Iterator & a, const Iterator & b)
            {
                return a.current_idx == b.current_idx;
            }
            friend bool operator!=(const Iterator & a, const Iterator & b)
            {
                return !(a == b);
            }

        private:
            friend class Object;
            Iterator(const Object & obj_, size_t idx_) 
                : object(&obj_), current_idx(idx_) {}

            const Object * object = nullptr;
            size_t current_idx = 0;
            
            mutable size_t cached_idx = static_cast<size_t>(-1);
            mutable const String * cached_key_ptr = nullptr;
            mutable Field cached_value;
        };

        Object() = default;
        ALWAYS_INLINE Object(const Field & obj_field_, const ColumnObject * col_obj_, size_t row_)
            : object_field(obj_field_), col_object(col_obj_), row(row_) {} /// NOLINT

        ALWAYS_INLINE Iterator begin() const { return Iterator(*this, 0); }
        ALWAYS_INLINE Iterator end() const { return Iterator(*this, size()); }
        
        ALWAYS_INLINE size_t size() const
        {
            if (object_field.getType() == Field::Types::Object)
                return object_field.safeGet<DB::Object>().size();
            return 0;
        }

        bool find(std::string_view key, Element & result) const; /// NOLINT
        bool findCaseInsensitive(std::string_view key, Element & result) const; /// NOLINT

    private:
        Field object_field;
        const ColumnObject * col_object = nullptr;
        size_t row = 0;
        mutable std::vector<std::string_view> all_keys;

        friend class Iterator;
    };

    /// Parse creates a root Element for the ColumnObject at given row.
    bool parse(std::string_view, Element & element)
    {
        element = Element();
        return true;
    }
};

}
