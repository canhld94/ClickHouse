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

/// Parser that reads directly from ColumnObject without serialization.
/// Implements the same Element/Array/Object API as SimdJSONParser
/// but operates on ColumnObject's internal structured data.
class ColumnObjectParser
{
public:
    class Array;
    class Object;

    /// Represents a JSON value from ColumnObject
    class Element {
    public:
        /// Returns the ElementType of the value
        ElementType type() const;
        Element() = default;

        /// Create element from root of ColumnObject at given row
        Element(const ColumnObject & col_obj_, size_t row_)
            : col_object(&col_obj_), row(row_), is_root(true) {}

        /// Create element from nested value (internal use)
        Element(const ColumnObject * col_obj_, size_t row_, const Field & field_val, bool is_root_ = false)
            : col_object(col_obj_), row(row_), field_value(field_val), is_root(is_root_) {}

        /// Type checking methods
        bool isInt64() const;
        bool isUInt64() const;
        bool isDouble() const;
        bool isBool() const;
        bool isString() const;
        bool isArray() const;
        bool isObject() const;
        bool isNull() const;

        /// Value accessor methods
        Int64 getInt64() const;
        UInt64 getUInt64() const;
        double getDouble() const;
        bool getBool() const;
        std::string_view getString() const;
        Array getArray() const;
        Object getObject() const;

        Element getElement() const { return *this; }

    private:
        const ColumnObject * col_object = nullptr;
        size_t row = 0;
        mutable Field field_value;
        bool is_root = false;

        friend class Array;
        friend class Object;
    };

    /// References an array in JSON
    class Array
    {
    public:
        class Iterator
        {
        public:
            Element operator*() const;
            Iterator & operator++();
            Iterator operator++(int);

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

        Array() = default;
        Array(const Field & arr_field_, const ColumnObject * col_obj_, size_t row_);

        Iterator begin() const { return Iterator(*this, 0); }
        Iterator end() const { return Iterator(*this, size()); }
        size_t size() const;
        Element operator[](size_t index) const;

    private:
        Field array_field;
        const ColumnObject * col_object = nullptr;
        size_t row = 0;

        friend class Iterator;
    };

    using KeyValuePair = std::pair<std::string_view, Element>;

    /// References a JSON object
    class Object
    {
    public:
        class Iterator
        {
        public:
            KeyValuePair operator*() const;
            Iterator & operator++();
            Iterator operator++(int);

            friend bool operator==(const Iterator & a, const Iterator & b)
            {
                return a.path_idx == b.path_idx;
            }
            friend bool operator!=(const Iterator & a, const Iterator & b)
            {
                return a.path_idx != b.path_idx;
            }

        private:
            friend class Object;
            Iterator(const Object & obj_, size_t idx_);

            const Object * object = nullptr;
            size_t path_idx = 0;
        };

        Object() = default;
        Object(const Field & obj_field_, const ColumnObject * col_obj_, size_t row_);

        Iterator begin() const { return Iterator(*this, 0); }
        Iterator end() const;
        size_t size() const;

        bool find(std::string_view key, Element & result) const; /// NOLINT
        bool findCaseInsensitive(std::string_view key, Element & result) const; /// NOLINT

    private:
        Field object_field;
        const ColumnObject * col_object = nullptr;
        size_t row = 0;
        mutable std::vector<std::string_view> all_keys;

        friend class Iterator;
    };

    /// Parse creates a root Element for the ColumnObject at given row
    bool parse(std::string_view, Element & element)
    {
        element = Element();
        return true;
    }
};

}
