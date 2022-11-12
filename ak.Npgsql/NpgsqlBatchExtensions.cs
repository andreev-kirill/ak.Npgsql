using ak.Npgsql.Extensions.Helpers;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace ak.Npgsql
{
    public static class NpgsqlBatchExtensions
    {
        public static void AddInsertCommands<T>(this NpgsqlBatchCommandCollection commandCollection,
            string tableName,
            IEnumerable<T> collection,
            Expression<Func<T, object>>[] checkExistColumns,
            bool useLowerCaseNaming = true,
            params Expression<Func<T, object>>[] excludeColumns)
        {
            AddInsertCommands(commandCollection, tableName, collection,
                checkExistColumns == null ? new string[0] :
                checkExistColumns.Select(e => e.GetName()).ToArray(), useLowerCaseNaming,
                excludeColumns.Select(e => e.GetName()).ToArray());
        }

        public static void AddInsertCommands<T>(this NpgsqlBatchCommandCollection commandCollection,
            string tableName, 
            IEnumerable<T> collection, 
            string[] checkExistColumns, 
            bool useLowerCaseNaming = true, 
            params string[] excludeColumns)
        {
            var lowerExclude = excludeColumns.Select(e => e.ToLower()).ToArray();
            var byPropertyGetter = typeof(T).GetProperties()
                .Where(p => excludeColumns.Length == 0 || !lowerExclude.Contains(p.Name.ToLower()))
                .Select(e => (name: useLowerCaseNaming ? e.Name.ToLower() : e.Name, valueGetter: new Func<object, object>(val => e.GetValue(val))));
            string columns = string.Join(",", byPropertyGetter.Select(q => q.name));
            string parameters = string.Join(",", byPropertyGetter.Select(q => $"@{q.name}"));
            var whereQuery = string.Join(" and ", checkExistColumns.Select(q => $"t.{(useLowerCaseNaming ? q.ToLower() : q) } = @{(useLowerCaseNaming ? q.ToLower() : q)}"));
            var commands = checkExistColumns?.Length > 0 
                ? collection.Select(e => new NpgsqlBatchCommand($"insert into {tableName}({columns}) select {parameters} where not exists(select 1 from {tableName} t where {whereQuery})").BuildParameters(e, byPropertyGetter))
                :  collection.Select(e => new NpgsqlBatchCommand($"insert into {tableName}({columns}) values({parameters})").BuildParameters(e, byPropertyGetter));
            foreach (var item in commands)
            {
                commandCollection.Add(item);
            }
        }
        public static NpgsqlBatch CreateBatchInsert<T>(this NpgsqlConnection conn, 
            string tblName, 
            IEnumerable<T> data, 
            bool useLowerCaseNaming = true,
            params Expression<Func<T, object>>[] excludeColumns)
        {
            var batch = new NpgsqlBatch(conn);
            batch.BatchCommands.AddInsertCommands(useLowerCaseNaming ? tblName.ToLower() : tblName, data, null, useLowerCaseNaming, excludeColumns);
            return batch;
        }
        public static NpgsqlBatch CreateBatchInsertWhithExistCheck<T>(this NpgsqlConnection conn, 
            string tblName, IEnumerable<T> data,
            Expression<Func<T, object>>[] checkExistColumns,
            bool useLowerCaseNaming = true, 
            params Expression<Func<T, object>>[] excludeColumns)
        {
            var batch = new NpgsqlBatch(conn);
            batch.BatchCommands.AddInsertCommands(useLowerCaseNaming ? tblName.ToLower() : tblName, data, checkExistColumns, useLowerCaseNaming, excludeColumns);
            return batch;
        }
        public static NpgsqlBatchCommand BuildParameters<T>(this NpgsqlBatchCommand command, 
            T value,
            bool useLowerCaseNaming = true,
            params Expression<Func<T, object>>[] excludeColumns)
        {
            var lowerExclude = excludeColumns.Select(e => e.GetName().ToLower()).ToArray();
            var byPropertyGetter = typeof(T).GetProperties()
                .Where(p => excludeColumns.Length == 0 || !lowerExclude.Contains(p.Name.ToLower()))
                .Select(e => (name: useLowerCaseNaming ? e.Name.ToLower() : e.Name, valueGetter: new Func<object, object>(val => e.GetValue(val))));
            command.BuildParameters(value, byPropertyGetter);
            return command;
        }
        private static NpgsqlBatchCommand BuildParameters<T>(this NpgsqlBatchCommand command,
            T value,
            IEnumerable<(string name, Func<object, object> valueGetter)> propertys)
        {
            foreach (var item in propertys)
            {
                var propertyValue = value == null ? null : item.valueGetter(value);
                command.Parameters.AddWithValue(item.name, propertyValue == null ? DBNull.Value : propertyValue);
            }

            return command;
        }
        
    }
}
