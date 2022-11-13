using ak.Npgsql.Extensions.Helpers;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace ak.Npgsql.Extensions
{
    public static class NpgsqlExtensions
    {
        public static NpgsqlCommand BuildParameters<T>(this NpgsqlCommand command,
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
        private static NpgsqlCommand BuildParameters<T>(this NpgsqlCommand command,
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
