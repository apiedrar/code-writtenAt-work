// remove_mismatching_rows.cpp
// Compile: g++ -std=c++17 -O3 -o remove_mismatching_rows remove_mismatching_rows.cpp
// Usage: ./remove_mismatching_rows input1.csv input2.csv output.csv --keys col1,col2

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>
#include <filesystem>
#include <stdexcept>

namespace fs = std::filesystem;

// Custom hash function for vector<string> to use in unordered_set
struct VectorHash
{
    std::size_t operator()(const std::vector<std::string> &v) const
    {
        std::size_t seed = v.size();
        for (const auto &str : v)
        {
            seed ^= std::hash<std::string>{}(str) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};

class DataFrame
{
private:
    std::vector<std::string> columns_;
    std::vector<std::vector<std::string>> rows_;
    std::unordered_map<std::string, size_t> column_index_;

public:
    DataFrame() = default;

    // Read CSV file
    static DataFrame read_csv(const std::string &filepath)
    {
        DataFrame df;
        std::ifstream file(filepath);

        if (!file.is_open())
        {
            throw std::runtime_error("Cannot open file: " + filepath);
        }

        std::string line;
        bool first_row = true;

        while (std::getline(file, line))
        {
            // Remove carriage return if present
            if (!line.empty() && line.back() == '\r')
            {
                line.pop_back();
            }

            std::vector<std::string> row;
            std::stringstream ss(line);
            std::string cell;

            while (std::getline(ss, cell, ','))
            {
                // Basic CSV parsing (doesn't handle quoted fields with commas)
                // Trim whitespace
                cell.erase(0, cell.find_first_not_of(" \t"));
                cell.erase(cell.find_last_not_of(" \t") + 1);
                row.push_back(cell);
            }

            if (first_row)
            {
                df.columns_ = row;
                for (size_t i = 0; i < row.size(); ++i)
                {
                    df.column_index_[row[i]] = i;
                }
                first_row = false;
            }
            else
            {
                if (!row.empty())
                {
                    df.rows_.push_back(row);
                }
            }
        }

        return df;
    }

    // Write CSV file
    void write_csv(const std::string &filepath) const
    {
        // Create directory if it doesn't exist
        fs::path path(filepath);
        if (path.has_parent_path())
        {
            fs::create_directories(path.parent_path());
        }

        std::ofstream file(filepath);
        if (!file.is_open())
        {
            throw std::runtime_error("Cannot create output file: " + filepath);
        }

        // Write header
        for (size_t i = 0; i < columns_.size(); ++i)
        {
            file << columns_[i];
            if (i < columns_.size() - 1)
                file << ",";
        }
        file << "\n";

        // Write rows
        for (const auto &row : rows_)
        {
            for (size_t i = 0; i < row.size(); ++i)
            {
                file << row[i];
                if (i < row.size() - 1)
                    file << ",";
            }
            file << "\n";
        }
    }

    // Get column index
    size_t get_column_index(const std::string &col_name) const
    {
        auto it = column_index_.find(col_name);
        if (it == column_index_.end())
        {
            throw std::runtime_error("Column not found: " + col_name);
        }
        return it->second;
    }

    // Check if column exists
    bool has_column(const std::string &col_name) const
    {
        return column_index_.find(col_name) != column_index_.end();
    }

    // Get all column names
    const std::vector<std::string> &columns() const { return columns_; }

    // Get row count
    size_t row_count() const { return rows_.size(); }

    // Get column count
    size_t column_count() const { return columns_.size(); }

    // Get row data
    const std::vector<std::vector<std::string>> &rows() const { return rows_; }

    // Extract key values from a row
    std::vector<std::string> extract_keys(const std::vector<std::string> &row,
                                          const std::vector<size_t> &key_indices) const
    {
        std::vector<std::string> keys;
        keys.reserve(key_indices.size());
        for (size_t idx : key_indices)
        {
            if (idx < row.size())
            {
                keys.push_back(row[idx]);
            }
            else
            {
                keys.push_back("");
            }
        }
        return keys;
    }

    // Filter rows based on matching keys
    DataFrame filter_matching_rows(const DataFrame &reference,
                                   const std::vector<std::string> &key_columns) const
    {
        // Get key column indices for both dataframes
        std::vector<size_t> key_indices_this;
        std::vector<size_t> key_indices_ref;

        for (const auto &col : key_columns)
        {
            key_indices_this.push_back(this->get_column_index(col));
            key_indices_ref.push_back(reference.get_column_index(col));
        }

        // Build hash set of keys from reference dataframe
        std::unordered_set<std::vector<std::string>, VectorHash> reference_keys;
        for (const auto &row : reference.rows())
        {
            auto keys = reference.extract_keys(row, key_indices_ref);
            reference_keys.insert(std::move(keys));
        }

        // Filter this dataframe
        DataFrame result;
        result.columns_ = this->columns_;
        result.column_index_ = this->column_index_;

        for (const auto &row : this->rows_)
        {
            auto keys = this->extract_keys(row, key_indices_this);
            if (reference_keys.find(keys) != reference_keys.end())
            {
                result.rows_.push_back(row);
            }
        }

        return result;
    }
};

// Parse command line arguments
struct Arguments
{
    std::string input1;
    std::string input2;
    std::string output;
    std::vector<std::string> keys;
};

Arguments parse_arguments(int argc, char *argv[])
{
    Arguments args;

    if (argc < 5)
    {
        std::cerr << "Usage: " << argv[0] << " input1.csv input2.csv output.csv --keys col1,col2\n";
        std::cerr << "\nExamples:\n";
        std::cerr << "  " << argv[0] << " data1.csv data2.csv output.csv --keys id\n";
        std::cerr << "  " << argv[0] << " file1.csv file2.csv result.csv --keys name,email\n";
        throw std::runtime_error("Invalid arguments");
    }

    args.input1 = argv[1];
    args.input2 = argv[2];
    args.output = argv[3];

    // Find --keys argument
    for (int i = 4; i < argc; ++i)
    {
        std::string arg = argv[i];
        if (arg == "--keys" || arg == "-k")
        {
            if (i + 1 < argc)
            {
                std::string keys_str = argv[i + 1];
                std::stringstream ss(keys_str);
                std::string key;
                while (std::getline(ss, key, ','))
                {
                    // Trim whitespace
                    key.erase(0, key.find_first_not_of(" \t"));
                    key.erase(key.find_last_not_of(" \t") + 1);
                    if (!key.empty())
                    {
                        args.keys.push_back(key);
                    }
                }
                break;
            }
            else
            {
                throw std::runtime_error("--keys requires an argument");
            }
        }
    }

    if (args.keys.empty())
    {
        throw std::runtime_error("--keys argument is required");
    }

    return args;
}

// Validate columns exist in dataframe
void validate_columns(const DataFrame &df, const std::vector<std::string> &columns,
                      const std::string &filename)
{
    std::vector<std::string> missing;
    for (const auto &col : columns)
    {
        if (!df.has_column(col))
        {
            missing.push_back(col);
        }
    }

    if (!missing.empty())
    {
        std::cerr << "Error: Columns [";
        for (size_t i = 0; i < missing.size(); ++i)
        {
            std::cerr << missing[i];
            if (i < missing.size() - 1)
                std::cerr << ", ";
        }
        std::cerr << "] not found in " << filename << "\n";
        std::cerr << "Available columns: [";
        const auto &cols = df.columns();
        for (size_t i = 0; i < cols.size(); ++i)
        {
            std::cerr << cols[i];
            if (i < cols.size() - 1)
                std::cerr << ", ";
        }
        std::cerr << "]\n";
        throw std::runtime_error("Column validation failed");
    }
}

int main(int argc, char *argv[])
{
    try
    {
        // Parse arguments
        Arguments args = parse_arguments(argc, argv);

        std::cout << "Reading input files...\n";
        std::cout << "  Input 1 (reference): " << args.input1 << "\n";
        std::cout << "  Input 2 (comparison): " << args.input2 << "\n";
        std::cout << "  Primary key columns: [";
        for (size_t i = 0; i < args.keys.size(); ++i)
        {
            std::cout << args.keys[i];
            if (i < args.keys.size() - 1)
                std::cout << ", ";
        }
        std::cout << "]\n";

        // Read input files
        DataFrame df1 = DataFrame::read_csv(args.input1);
        DataFrame df2 = DataFrame::read_csv(args.input2);

        std::cout << "\nInput file statistics:\n";
        std::cout << "  " << args.input1 << ": " << df1.row_count()
                  << " rows, " << df1.column_count() << " columns\n";
        std::cout << "  " << args.input2 << ": " << df2.row_count()
                  << " rows, " << df2.column_count() << " columns\n";

        // Validate columns
        validate_columns(df1, args.keys, args.input1);
        validate_columns(df2, args.keys, args.input2);

        // Match rows
        std::cout << "\nMatching rows based on primary keys...\n";
        DataFrame matched_df = df1.filter_matching_rows(df2, args.keys);

        // Write output
        matched_df.write_csv(args.output);

        std::cout << "\nResults:\n";
        std::cout << "  Original rows in " << args.input1 << ": " << df1.row_count() << "\n";
        std::cout << "  Matching rows found: " << matched_df.row_count() << "\n";
        std::cout << "  Rows removed: " << (df1.row_count() - matched_df.row_count()) << "\n";
        std::cout << "  Output saved to: " << args.output << "\n";

        return 0;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}