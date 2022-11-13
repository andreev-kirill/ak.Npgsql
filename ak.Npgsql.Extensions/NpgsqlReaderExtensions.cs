using Npgsql;
using Npgsql.PostgresTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ak.Npgsql
{
    public static class NpgsqlReaderExtensions
    {
        public static async Task<IEnumerable<T>> QueryAsync<T>(this NpgsqlConnection connection, string commandText, object parameters = null)
        {
            List<T> result = new List<T>();
            var propertyInfo = GetPropertyDictionary<T>();
            var tCreate = GetCreateFunc<T>();
            var command = connection.PrepareCommand(commandText, parameters);
            using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                result.Add(reader.CreateFilledItem(tCreate, propertyInfo));
            }
            command.Dispose();
            return result;
        }
        public static async Task<IEnumerable<T>> QueryAsync<T>(this NpgsqlBatch batch)
        {
            List<T> result = new List<T>();
            var propertyInfo = GetPropertyDictionary<T>();
            var tCreate = GetCreateFunc<T>();
            using NpgsqlDataReader reader = await batch.ExecuteReaderAsync();
            do
            {
                while (await reader.ReadAsync())
                {
                    result.Add(reader.CreateFilledItem(tCreate, propertyInfo));
                }
            } while (await reader.NextResultAsync());
            return result;
        }
        public static async Task ExecuteNonQueryAsync(this NpgsqlConnection connection, string commandText, object parameters)
        {
            var command = connection.PrepareCommand(commandText, parameters);
            await command.ExecuteNonQueryAsync();
            command.Dispose();
        }
        private static NpgsqlCommand PrepareCommand(this NpgsqlConnection connection,
            string commandText,
            object parameters)
        {
            var cmd = new NpgsqlCommand(commandText, connection);
            if (parameters != null)
                foreach (var parameter in parameters.GetType().GetProperties())
                {
                    var value = parameter.GetValue(parameters);
                    cmd.Parameters.AddWithValue(parameter.Name, value ?? DBNull.Value);
                }

            return cmd;
        }
        public static async Task<T> QuerySingleAsync<T>(this NpgsqlConnection connection, string commandText, object parameters)
        {
            T item = default(T);
            var propertyInfo = GetPropertyDictionary<T>();
            var tCreate = GetCreateFunc<T>();
            var command = connection.PrepareCommand(commandText, parameters);
            using var reader = await command.ExecuteReaderAsync();
            int i = 0;
            while (await reader.ReadAsync())
            {
                item = reader.CreateFilledItem(tCreate, propertyInfo);
                i++;
                if (i > 1) throw new Exception("more than one element");
            }
            command.Dispose();
            return item;
        }

        public static async Task<T> QueryFirstOrDefaultAsync<T>(this NpgsqlConnection connection, string commandText, object parameters)
        {
            var command = connection.PrepareCommand(commandText, parameters);
            var propertyInfo = GetPropertyDictionary<T>();
            var tCreate = GetCreateFunc<T>();
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var item = reader.CreateFilledItem(tCreate, propertyInfo);
                command.Dispose();
                return item;
            }
            command.Dispose();
            return default(T);
        }
        private static Dictionary<string, PropertyInfo> GetPropertyDictionary<T>()
        {
            return typeof(T).GetProperties().ToDictionary(e => e.Name.ToLower());
        }
        private static Func<T> GetCreateFunc<T>()
        {
            var typeCode = Type.GetTypeCode(typeof(T));
            if (typeof(T).IsPrimitive
                || typeCode == TypeCode.String
                || typeCode == TypeCode.Decimal
                || typeCode == TypeCode.DateTime)
                return null;
            NewExpression constructorExpression = Expression.New(typeof(T));
            Expression<Func<T>> lambdaExpression = Expression.Lambda<Func<T>>(constructorExpression);
            return lambdaExpression.Compile();
        }
        private static T CreateFilledItem<T>(this NpgsqlDataReader reader, 
            Func<T> createFunc,
            Dictionary<string, PropertyInfo> fillList)
        {
            if (createFunc == null)
                return reader.GetFieldValue<T>(0);
            var item = createFunc();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (fillList.TryGetValue(reader.GetName(i).ToLower(), out var prop))
                {
                    var value = reader[i];
                    if (value != DBNull.Value)
                        prop.SetValue(item, ChangeType(reader[i], prop.PropertyType));
                }
            }
            return item;
        }
        public static object ChangeType(object value, Type conversion)
        {
            var t = conversion;

            if (t.IsGenericType && t.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
            {
                if (value == null)
                {
                    return null;
                }

                t = Nullable.GetUnderlyingType(t);
            }

            return Convert.ChangeType(value, t);
        }
    }
}
