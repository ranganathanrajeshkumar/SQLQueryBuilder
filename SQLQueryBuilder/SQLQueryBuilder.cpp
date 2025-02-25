#include <iostream>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

enum class DatabaseType {
    Oracle,
    MariaDB
};

class SQLQueryBuilder {
public:
    SQLQueryBuilder(DatabaseType dbType) : dbType(dbType) {
        InitializeReservedKeywords();
    }

    SQLQueryBuilder& Select(const std::vector<std::string>& fields) {
        for (const auto& field : fields) {
            selectFields.push_back(HandleReservedKeyword(field));
        }
        return *this;
    }

    SQLQueryBuilder& From(const std::string& table) {
        tableName = table;
        return *this;
    }

    SQLQueryBuilder& Distinct() 
    {
        is_distinct = true;
        return *this;
    }

    SQLQueryBuilder& Where(const std::vector<std::pair<std::string, std::string>>& conditions, bool isDateTime = false) {
        for (const auto& condition : conditions) {
            std::string formattedValue = condition.second;
            if (isDateTime) {
                formattedValue = FormatDateTime(condition.second);
            }
            whereConditions.push_back(HandleReservedKeyword(condition.first) + " = " + formattedValue);
        }
        return *this;
    }

    SQLQueryBuilder& WhereWithPlaceholder(const std::vector<std::pair<std::string, std::string>>& conditions) {
        for (const auto& condition : conditions) {
            whereConditions.push_back(HandleReservedKeyword(condition.first) + " = " + condition.second);
        }
        return *this;
    }

    template<typename T>
    SQLQueryBuilder& SetValue(const std::string& placeholder, const T& value) {
        values[placeholder] = ConvertToString(value);
        return *this;
    }

    SQLQueryBuilder& InnerJoin(const std::string& table, const std::string& onCondition) {
        joins.push_back("INNER JOIN " + table + " ON " + onCondition);
        return *this;
    }

    SQLQueryBuilder& OrderBy(const std::string& field, bool ascending = true) {
        orderBy = HandleReservedKeyword(field) + (ascending ? " ASC" : " DESC");
        return *this;
    }

    SQLQueryBuilder& Index(const std::string& indexName) {
        indexClause = indexName;
        return *this;
    }

    SQLQueryBuilder& Limit(int limit) {
        this->limit = limit;
        return *this;
    }

    SQLQueryBuilder& Offset(int offset) {
        this->offset = offset;
        return *this;
    }

    std::string Build() const {
        std::ostringstream query;
        query << "SELECT ";

        if (dbType == DatabaseType::Oracle && !indexClause.empty())
        {
            query << " /*+ INDEX(" + tableName + ", " + indexClause + ") */ ";
        }

        if (selectFields.empty()) 
        {
            query << "*";
        }
        else 
        {
            if (is_distinct)
            {
                query << " DISTINCT  ";
            }
            query << Join(selectFields, ", ");
        }

        query << " FROM " << tableName;

        if (dbType == DatabaseType::MariaDB && !indexClause.empty())
        {
            query << " FORCE INDEX(" << indexClause << ") ";
        }

        for (const auto& join : joins) {
            query << " " << join;
        }

        if (!whereConditions.empty()) {
            std::string whereClause = Join(whereConditions, " AND ");
            for (const auto& pair : values) {
                size_t pos = whereClause.find(pair.first);
                if (pos != std::string::npos) {
                    whereClause.replace(pos, pair.first.length(), pair.second);
                }
            }
            query << " WHERE " << whereClause;
        }

        if (!orderBy.empty()) {
            query << " ORDER BY " << orderBy;
        }

        if (dbType == DatabaseType::MariaDB) {
            if (limit >= 0) {
                query << " LIMIT " << limit;
                if (offset > 0) {
                    query << " OFFSET " << offset;
                }
            }
        }
        else if (dbType == DatabaseType::Oracle) {
            if (limit >= 0) {
                query << " FETCH FIRST " << limit << " ROWS ONLY";
            }
        }

        return query.str();
    }

private:
    DatabaseType dbType;
    std::vector<std::string> selectFields;
    std::string tableName;
    std::vector<std::string> whereConditions;
    std::vector<std::string> joins;
    std::string orderBy;
    std::string indexClause;
    bool is_distinct = false;
    int limit = -1;
    int offset = -1;
    std::unordered_map<std::string, std::string> values;
    std::unordered_set<std::string> reservedKeywords;

    void InitializeReservedKeywords() {
        reservedKeywords = { "DATE", "USER", "ORDER", "GROUP", "INDEX" };
    }

    static std::string Join(const std::vector<std::string>& elements, const std::string& delimiter) {
        std::ostringstream result;
        for (size_t i = 0; i < elements.size(); ++i) {
            if (i > 0) result << delimiter;
            result << elements[i];
        }
        return result.str();
    }

    template<typename T>
    std::string ConvertToString(const T& value) const {
        std::ostringstream oss;
        oss << value;
        return oss.str();
    }

    std::string HandleReservedKeyword(const std::string& field) const {
        if (reservedKeywords.find(field) != reservedKeywords.end()) {
            if (dbType == DatabaseType::MariaDB) {
                return "`" + field + "`";
            }
            else { // Oracle
                return "\"" + field + "\"";
            }
        }
        return field;
    }

    std::string FormatDateTime(const std::string& dateTime) const
    {
        if (dbType == DatabaseType::MariaDB)
        {
            return "'" + dateTime + "'";
        }
        else
        { // Oracle
            return "TO_TIMESTAMP('" + dateTime + "', 'YYYY-MM-DD HH24:MI:SS')";
        }
    }
};

// Example Usage
int main() {
    SQLQueryBuilder builder(DatabaseType::MariaDB);
    std::string query = builder.Select({ "id", "name", "DATE" })
        .Distinct()
        .From("users")
        .Index("idx_users_name")
        .WhereWithPlaceholder({ {"join_date", "?joindate"} })
        .SetValue("?joindate", "SYSDATE")
        .InnerJoin("orders", "users.id = orders.user_id")
        .OrderBy("name")
        .Limit(10)
        .Offset(5)
        .Build();

    std::cout << "Generated Query: " << query << std::endl;
    return 0;
}
