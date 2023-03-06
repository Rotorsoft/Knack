using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Knak;

namespace Knak.Data {
	public delegate T ReaderCallback<T>(ReaderExecutionContext<T> context) where T : class, new();

	#region Interfaces

	public interface ICommand { string GetScript(); }

	public interface IExecutionContext : IDisposable {
		IConnection Connection { get; }
		string CommandName { get; }
		string Script { get; }
		Exception Error { get; }
	}

    public class CommandExecutionEventArgs : EventArgs {
        public IExecutionContext Context { get; private set; }
        public bool IgnoreErrors { get; set; }

        internal CommandExecutionEventArgs(IExecutionContext context) {
            Context = context;
            IgnoreErrors = false;
        }
    }

	public interface IRepository<T, K> {
		IEnumerable<T> LoadAll();
		T Load(K key);
		int Save(T instance);
		int Delete(K key);
	}

	public interface ICommandDeployer {	void Deploy(IConnection connection, IEnumerable<ICommand> commands); }

    public interface IDbProvider {
        IDbConnection CreateConnection();
        IDbCommand CreateCommand();
		IDbDataParameter CreateDataParameter(string name, DbType dbType, ParameterDirection direction, int size, byte precision, byte scale, bool isNullable, string udtName, object value);
	}

	public interface IConnection : IDisposable {
		IDbProvider DbProvider { get; }
		string ConnectionString { get; }
		int TimeOut { get; set; }
		CommandType ExecutionMode { get; set; }
        bool AutoClose { get; set; }

		event EventHandler<CommandExecutionEventArgs> BeforeExecute;
		event EventHandler<CommandExecutionEventArgs> AfterExecute;
        void OnBeforeExecute(IExecutionContext context);
        void OnAfterExecute(IExecutionContext context);

		string GetName(ICommand cmd);

		IDbConnection Open();
		void Close();

 		/// <summary>
		/// Executes command invoking ADO.NET Command.ExecuteNonQuery internally.
		/// </summary>
		/// <returns>The number of records affected.</returns>
		int Execute(ICommand cmd);

		/// <summary>
		/// Executes command invoking ADO.NET Command.ExecuteScalar internally.
		/// </summary>
		/// <returns>The first column of the first row in the resultset as T. Extra columns or rows are ignored.</returns>
		T ExecuteScalar<T>(ICommand cmd);

		/// <summary>
		/// Executes command as data reader, materializing all records into a list of instances of T.
		/// </summary>
		IEnumerable<T> ExecuteReader<T>(ICommand cmd, ReaderCallback<T> callback = null) where T : class, new();
	}

	#endregion Interfaces

    [AttributeUsage(AttributeTargets.Class)]
    public class NamespaceAttribute : Attribute {
        public string Namespace { get; private set; }
        public NamespaceAttribute(string name) { Namespace = name; }
    }

	[AttributeUsage(AttributeTargets.Property)]
	public class ParameterAttribute : Attribute {
		private int _size = -1; // varchar(MAX), varbinary(MAX)
		public int Size { get { return _size; } private set { _size = value; } }
		public byte Precision { get; private set; }
		public byte Scale { get; private set; }
		public bool IsNullableString { get; private set; } // string?

		/// <summary>
		/// Creates a new (non-nullable) string or binary parameter attribute. Size is -1 by default for varchar(MAX) and varbinary(MAX)
		/// </summary>
		public ParameterAttribute(int size) { Size = size; }

		/// <summary>
		/// Creates a new string parameter attribute.
		/// </summary>
		public ParameterAttribute(int size, bool isNullableString) {
			Size = size;
			IsNullableString = isNullableString;
		}

		/// <summary>
		/// Creates a new decimal parameter attribute. 
		/// </summary>
		public ParameterAttribute(byte precision, byte scale) {
			Precision = precision;
			Scale = scale;
		}
	}

	public class SqlClientDbProvider : IDbProvider {
		public IDbConnection CreateConnection() { return new System.Data.SqlClient.SqlConnection(); }
		public IDbCommand CreateCommand() { return new System.Data.SqlClient.SqlCommand(); }
		public IDbDataParameter CreateDataParameter(string name, DbType dbType, ParameterDirection direction, int size, byte precision, byte scale, bool isNullable, string udtName, object value) {
			SqlParameter p = new SqlParameter();
			p.ParameterName = name;
			if (udtName == null) p.DbType = dbType;
			else {
				p.SqlDbType = SqlDbType.Udt;
				p.UdtTypeName = udtName;
			}
			p.Direction = direction;
			p.Size = size;
			if (precision > 0) {
				p.Precision = precision;
				p.Scale = scale;
			}
			p.IsNullable = isNullable;
			Parameter.SetValue(p, value);
			return p;
		}
	}

    //public class SqlServerCeDbProvider : IDbProvider { // requires reference to System.Data.SqlServerCe v4.0
    //    public IDbConnection CreateConnection() { return new System.Data.SqlServerCe.SqlCeConnection(); }
    //    public IDbCommand CreateCommand() { return new System.Data.SqlServerCe.SqlCeCommand(); }
    //    public IDbDataParameter CreateDataParameter(string name, DbType dbType, ParameterDirection direction, int size, byte precision, byte scale, bool isNullable, string udtName, object value) {
    //        var p = new System.Data.SqlServerCe.SqlCeParameter();
    //        p.ParameterName = name;
    //        p.DbType = dbType;
    //        p.Direction = direction;
    //        p.Size = size;
    //        if (precision > 0) {
    //            p.Precision = precision;
    //            p.Scale = scale;
    //        }
    //        p.IsNullable = isNullable;
    //        Parameter.SetValue(p, value);
    //        return p;
    //    }
    //}

	public class OleDbDbProvider : IDbProvider {
		public IDbConnection CreateConnection() { return new System.Data.OleDb.OleDbConnection(); }
		public IDbCommand CreateCommand() { return new OleDbCommand(); }
		public IDbDataParameter CreateDataParameter(string name, DbType dbType, ParameterDirection direction, int size, byte precision, byte scale, bool isNullable, string udtName, object value) {
			var p = new OleDbParameter();
			p.ParameterName = name;
			p.DbType = dbType;
			p.Direction = direction;
			if (size > 0) p.Size = size;
			else if (precision > 0) {
				p.Precision = precision;
				p.Scale = scale;
			}
			p.IsNullable = isNullable;
			Parameter.SetValue(p, value);
			return p;
		}
	}

	public class OdbcDbProvider : IDbProvider {
		public IDbConnection CreateConnection() { return new System.Data.Odbc.OdbcConnection(); }
		public IDbCommand CreateCommand() { return new OdbcCommand(); }
		public IDbDataParameter CreateDataParameter(string name, DbType dbType, ParameterDirection direction, int size, byte precision, byte scale, bool isNullable, string udtName, object value) {
			var p = new OdbcParameter();
			p.ParameterName = name;
			p.DbType = dbType;
			p.Direction = direction;
			if (size > 0) p.Size = size;
			else if (precision > 0) {
				p.Precision = precision;
				p.Scale = scale;
			}
			p.IsNullable = isNullable;
			Parameter.SetValue(p, value);
			return p;
		}
	}

	public class Connection : IConnection {
		public Connection(string connectionString, IDbProvider dbProvider) {
			ConnectionString = connectionString;
			DbProvider = dbProvider;
			TimeOut = 30000; // 30 secs timeout by default
			AutoClose = true; // auto close by default
		}

		public IDbProvider DbProvider { get; private set; }
		public string ConnectionString { get; private set; }
		public int TimeOut { get; set; }
		public CommandType ExecutionMode { get; set; }
		public bool AutoClose { get; set; }

		public void Dispose() { Close(); }

		private IDbConnection dbConnection;
		public IDbConnection Open() {
			if (dbConnection == null) {
				dbConnection = DbProvider.CreateConnection();
				dbConnection.ConnectionString = ConnectionString;
			}
			if (dbConnection.State == ConnectionState.Closed)
				dbConnection.Open();
			return dbConnection;
		}

		public void Close() {
			if (dbConnection != null && dbConnection.State == ConnectionState.Open)
				dbConnection.Close();
		}

		public event EventHandler<CommandExecutionEventArgs> BeforeExecute;
		public event EventHandler<CommandExecutionEventArgs> AfterExecute;

        public void OnBeforeExecute(IExecutionContext context) {
            if (BeforeExecute != null) BeforeExecute(this, new CommandExecutionEventArgs(context));
        }

        public void OnAfterExecute(IExecutionContext context) {
            if (AfterExecute != null) {
                var args = new CommandExecutionEventArgs(context);
                AfterExecute(this, args);
                if (args.IgnoreErrors) return; // filter exceptions
            }
            if (context.Error != null) throw context.Error; // throw exceptions if not filtered by AfterExecute handlers
        }

		public virtual string GetName(ICommand command) {
			var cmdName = command.GetType().FullName.Replace(".", "_");
			var nsAttr = command.GetType().GetCustomAttributes(typeof(NamespaceAttribute), false).FirstOrDefault() as NamespaceAttribute;
			var schemaName = nsAttr == null ? "dbo" : nsAttr.Namespace;
			return string.Concat("[", schemaName, "].[", cmdName, "]");
		}

		public int Execute(ICommand cmd) {
			using (var ctx = new ExecutionContext(this, cmd)) {
				try {
					ctx.Open();
					return ctx.ExecuteNonQuery();
				}
				catch (Exception ex) { ctx.HandleError(ex); }
			}
			return 0;
		}

        public T ExecuteScalar<T>(ICommand cmd) {
			using (var ctx = new ExecutionContext(this, cmd)) {
				try {
					ctx.Open();
					return ctx.ExecuteScalar<T>();
				}
				catch (Exception ex) { ctx.HandleError(ex); }
			}
			return default(T);
		}

		public IEnumerable<T> ExecuteReader<T>(ICommand cmd, ReaderCallback<T> callback = null) where T : class, new() {
			using (var ctx = new ReaderExecutionContext<T>(this, cmd)) {
				foreach (var t in ctx.ExecuteReader(callback)) yield return t; // enum again inside using block
			}
		}
	}

	public class ExecutionContext : IExecutionContext {
		public ICommand Command { get; private set; }
		public IConnection Connection { get; private set; }
		public string CommandName { get; protected set; }
		public string Script { get; protected set; }
		public IDbCommand DbCommand { get; protected set; }
        public Exception Error { get; protected set; }

		public void HandleError(Exception ex) { if (Error == null) Error = ex; }
		public void Dispose() { Close(); }

        public ExecutionContext(IConnection connection, ICommand command) {
			Connection = connection;
			Command = command;
			if (command != null) { // batch context doesn't use command
				CommandName = command.GetType().Name;
				Script = Command.GetScript();
			}
		}

 		public void Open() {
            Connection.OnBeforeExecute(this);
            OpenDbCommand();
		}

		protected virtual void Close() {
			try {
				if (DbCommand != null) {
					if (Command != null) DbCommand.Output(Command);
					if (Connection.AutoClose) Connection.Close();
				}
			}
			catch (Exception ex) { HandleError(ex); }
            Connection.OnAfterExecute(this);
        }

		protected virtual void OpenDbCommand() {
            DbCommand = Connection.DbProvider.DbCommand(Command);
			if (Connection.ExecutionMode != CommandType.StoredProcedure || Command is Script) { // scripts are always executed as text
				DbCommand.CommandType = CommandType.Text;
				DbCommand.CommandText = Script;
			}
			else {
				DbCommand.CommandType = CommandType.StoredProcedure;
				DbCommand.CommandText = Connection.GetName(Command);
			}
			DbCommand.CommandTimeout = Connection.TimeOut;
			OpenDbConnection();
		}

		protected void OpenDbConnection() { DbCommand.Connection = Connection.Open(); }

		public int ExecuteNonQuery() { return DbCommand.ExecuteNonQuery(); }
		public T ExecuteScalar<T>() { return (T)DbCommand.ExecuteScalar(); }
	}

	public class ReaderExecutionContext<T> : ExecutionContext where T : class, new() {
		public int ResultIndex { get; internal protected set; }
		public IDataRecord Record { get { return reader; } }

        public ReaderExecutionContext(IConnection connection, ICommand command) : base(connection, command) { }

		public T Materialize() { return Command.Read<T>(Record, ResultIndex - 1); }
		public I Materialize<I>() where I : class, new() { return Command.Read<I>(Record, ResultIndex - 1); }

		private IDataReader reader = null;
		internal IEnumerable<T> ExecuteReader(ReaderCallback<T> callback) {
			try {
				Open();
				reader = DbCommand.ExecuteReader();
			}
			catch (Exception ex) {
				HandleError(ex);
				yield break;
			}

			bool more = false, read = false;
			ResultIndex = 1;
			if (!reader.IsClosed) {
				do {
					do {
						T t = default(T);
						try { // hydrate t with next record
							read = reader.Read();
							if (read) t = callback != null ? callback(this) : Materialize();
						}
						catch (Exception ex) {
							HandleError(ex);
							yield break;
						}
						if (read && t != null) yield return t; // yield cannot be inside try/catch block
					} while (read);
					try { // check if there are more result sets
						more = reader.NextResult();
						ResultIndex++;
					}
					catch (Exception ex) {
						HandleError(ex);
						yield break;
					}
				} while (more);
			}
		}

		protected override void Close() {
			if (reader != null) {
				try {
					reader.Close();
					reader.Dispose();
				}
				catch (Exception ex) { HandleError(ex); }
				reader = null;
			}
			base.Close();
		}
	}

	public static class CommandMapper {
		public static IDbCommand DbCommand(this IDbProvider dbProvider, ICommand cmd) {
            if (cmd is Script) { // scripts are not cached
                var script = cmd as Script;
                var dbCommand = dbProvider.CreateCommand();
                if (script.Parameters != null) {
                    foreach (var sp in script.Parameters.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public)) {
                        var value = sp.GetValue(script.Parameters, null);
                        var p = new Parameter(sp.Name, value.GetType(), ParameterDirection.Input, null);
                        var dbp = dbProvider.CreateDataParameter(p.Name, p.DbType, p.Direction, p.Size, p.Precision, p.Scale, p.IsNullable, p.UdtName, value);
                        dbCommand.Parameters.Add(dbp);
                    }
                }
                return dbCommand;
            }
            return DbCommandMapper.GetEntry(cmd.GetType()).InMapper(dbProvider, cmd); 
        }
		
        public static void Output(this IDbCommand dbCmd, ICommand cmd) { 
			if(!(cmd is Script)) DbCommandMapper.GetEntry(cmd.GetType()).OutMapper(dbCmd, cmd); // scripts don't support output parameters
		}

		public static T Read<T>(this ICommand cmd, IDataRecord record, int index) where T : class, new() { return DataRecordMapper<T>.Map(cmd, record, index); }

		public static class Of<CommandT> where CommandT : ICommand {
			public static IEnumerable<IMapping> InMappings { get { return DbCommandMapper.GetEntry(typeof(CommandT)).InMappings; } }
			public static IEnumerable<IMapping> OutMappings { get { return DbCommandMapper.GetEntry(typeof(CommandT)).OutMappings; } }

			public static LambdaExpression GetInMapperExpression() { return GetMapperExpressionIn<DbCommandMapper.InMapperCallback>(typeof(CommandT), InMappings); }
			public static LambdaExpression GetOutMapperExpression() { return GetMapperExpressionOut<DbCommandMapper.OutMapperCallback>(typeof(CommandT), OutMappings); }

			public static class To<T> where T : class, new() {
				public static IEnumerable<IMapping> GetMappings(int index) {
					if (index >= 0 && index < MAXRECSETS) return DataRecordMapper<T>.LocateEntry(typeof(CommandT)).Mappings[index];
					return null;
				}

				public static LambdaExpression GetMapperExpression(int index) {
					if (index >= 0 && index < MAXRECSETS) return DataRecordMapper<T>.GetMapperExpression(GetMappings(index));
					return null;
				}
			}
		}

		#region IDbCommand Mapping

		static class DbCommandMapper {
			public delegate IDbCommand InMapperCallback(IDbProvider provider, ICommand cmd);
			public delegate void OutMapperCallback(IDbCommand dbCmd, ICommand cmd);

			public struct CacheEntry {
				public InMapperCallback InMapper;
				public OutMapperCallback OutMapper;
				public IEnumerable<ParameterMapping> InMappings;
				public IEnumerable<ParameterMapping> OutMappings;
			}

			static ConcurrentDictionary<Type, CacheEntry> cache = new ConcurrentDictionary<Type, CacheEntry>();
			public static CacheEntry GetEntry(Type key) {
				return cache.GetOrAdd(key, k => {
					var inmappings = GetMappings(key, false);
					var outmappings = GetMappings(key, true);
					return new CacheEntry {
						InMappings = inmappings,
						OutMappings = outmappings,
						InMapper = GetMapperExpressionIn<InMapperCallback>(key, inmappings).Compile(),
						OutMapper = GetMapperExpressionOut<OutMapperCallback>(key, outmappings).Compile()
					};
				});
			}
		}

		class ParameterMapping : IMapping {
			public string Source { get; private set; }
			public string Target { get; private set; }
			public Type SourceType { get; private set; }
			public Type TargetType { get; private set; }
			public Parameter Parameter { get; private set; }
			public ParameterMapping(string name, Type sourceType, Type targetType, Parameter parameter) {
				Source = Target = name;
				SourceType = sourceType;
				TargetType = targetType;
				Parameter = parameter;
			}
		}

		public static IEnumerable<Parameter> GetParameters(this Type type) {
			ParameterDirection direction;
			ParameterAttribute attr;
			foreach (var p in type.GetMappableProperties(true)) {
				bool input = p.GetCustomAttributes(typeof(InputAttribute), false).Length > 0;
				bool output = p.GetCustomAttributes(typeof(OutputAttribute), false).Length > 0;
				if (input && output) direction = ParameterDirection.InputOutput;
				else if (output) direction = ParameterDirection.Output;
				else direction = ParameterDirection.Input; // set input by default
                if (input  && !p.CanRead)  throw new Exception(string.Concat("Input property ", p.Name, " of type ", type.FullName, " is not readable."));
                if (output && !p.CanWrite) throw new Exception(string.Concat("Output property ", p.Name, " of type ", type.FullName, " is not writeable."));
				object[] attributes = p.GetCustomAttributes(typeof(ParameterAttribute), false);
				attr = attributes.Length > 0 ? (ParameterAttribute)attributes[0] : null;
				yield return new Parameter(p.Name, p.PropertyType, direction, attr);
			}
		}

		static IEnumerable<ParameterMapping> GetMappings(Type type, bool onlyOutput) {
			var mappings = new List<ParameterMapping>();
			foreach(var p in type.GetParameters()) {
				if (p.Direction == ParameterDirection.Input && onlyOutput) continue; // skip input parameters when for output only
				Type sourceType = onlyOutput ? typeof(IDbCommand) : type;
				Type targetType = onlyOutput ? type : typeof(IDbCommand);
				mappings.Add(new ParameterMapping(p.Name, sourceType, targetType, p));
			}
			return mappings;
		}

		static Expression<TDelegate> GetMapperExpressionIn<TDelegate>(Type sourceType, IEnumerable<IMapping> mappings) {
			var cmd = Expression.Parameter(typeof(ICommand), "cmd");
			var provider = Expression.Parameter(typeof(IDbProvider), "provider");
			var source = Expression.Parameter(sourceType, "source");
			var target = Expression.Parameter(typeof(IDbCommand), "target");
			var end = Expression.Label(typeof(IDbCommand), "end");
			var block = Expression.Block(new[] { source, target },
					Expression.Assign(source, Expression.Convert(cmd, sourceType)),
					Expression.Assign(target, Expression.Call(provider, typeof(IDbProvider).GetMethod("CreateCommand"))),
					Expression.Block(GetMappingExpressionsIn(mappings, provider, source, target)),
					Expression.Label(end, target));
			return Expression.Lambda<TDelegate>(block, "MapIn", new[] { provider, cmd });
		}

		static Expression<TDelegate> GetMapperExpressionOut<TDelegate>(Type targetType, IEnumerable<IMapping> mappings) {
			var cmd = Expression.Parameter(typeof(ICommand), "cmd");
			var source = Expression.Parameter(typeof(IDbCommand), "source");
			var target = Expression.Parameter(targetType, "target");
			var block = Expression.Block(Expression.Empty());
			if (mappings.Count() > 0)
				block = Expression.Block(new[] { target },
					Expression.Assign(target, Expression.Convert(cmd, targetType)),
					Expression.Block(GetMappingExpressionsOut(mappings, source, target)));
			return Expression.Lambda<TDelegate>(block, "MapOut", new[] { source, cmd });
		}

		static IEnumerable<Expression> GetMappingExpressionsIn(IEnumerable<IMapping> mappings, ParameterExpression provider, ParameterExpression source, ParameterExpression target) {
			foreach (ParameterMapping pm in mappings) {
				var p = pm.Parameter;
				var targetparam = Expression.Call(provider,
					typeof(IDbProvider).GetMethod("CreateDataParameter"),
					Expression.Constant(p.Name, typeof(string)),
					Expression.Constant(p.DbType, typeof(DbType)),
					Expression.Constant(p.Direction, typeof(ParameterDirection)),
					Expression.Constant(p.Size, typeof(int)),
					Expression.Constant(p.Precision, typeof(byte)),
					Expression.Constant(p.Scale, typeof(byte)),
					Expression.Constant(p.IsNullable, typeof(bool)),
					Expression.Constant(p.UdtName, typeof(string)),
					p.Direction == ParameterDirection.Output ? (Expression)Expression.Constant(null) : Expression.Convert(Expression.Property(source, p.Name), typeof(object))
				);
				var targetparams = Expression.Property(target, typeof(IDbCommand).GetProperty("Parameters"));
				yield return Expression.Call(targetparams, typeof(System.Collections.IList).GetMethod("Add"), targetparam);
			}
			yield return Expression.Empty();
		}

		static IEnumerable<Expression> GetMappingExpressionsOut(IEnumerable<IMapping> mappings, ParameterExpression source, ParameterExpression target) {
			foreach (ParameterMapping pm in mappings) {
				var p = pm.Parameter;
				var targetp = Expression.Property(target, p.Name);
				var parameters = Expression.Property(source, typeof(IDbCommand).GetProperty("Parameters"));
				var itemprop = typeof(IDataParameterCollection).GetProperty("Item", new[] { typeof(string) });
				var targetparam = Expression.Convert(Expression.Property(parameters, itemprop, Expression.Constant(p.Name)), typeof(IDbDataParameter));
				var getter = Expression.Convert(Expression.Call(typeof(Parameter).GetMethod("GetValue"), targetparam), p.Type);
				yield return Expression.Assign(targetp, getter);
			}
		}

		#endregion IDbCommand Mapping

		#region IDataRecord Mapping

		const int MAXRECSETS = 10;

		static class DataRecordMapper<T> where T : class, new() {
			public delegate T MapperCallback(IDataRecord source);

			public struct CacheEntry {
				public MapperCallback[] Mappers;
				public IEnumerable<IMapping>[] Mappings;
			}

			static ConcurrentDictionary<int, CacheEntry> cache = new ConcurrentDictionary<int, CacheEntry>();
			static CacheEntry GetEntry(int key) { return cache.GetOrAdd(key, new CacheEntry { Mappers = new MapperCallback[MAXRECSETS], Mappings = new IEnumerable<IMapping>[MAXRECSETS] }); }

			public static CacheEntry LocateEntry(Type cmdType) { return GetEntry(cmdType.GetHashCode()); }

			public static Expression<MapperCallback> GetMapperExpression(IEnumerable<IMapping> mappings) {
				if (mappings != null) return GetMapperExpression<MapperCallback>(typeof(T), mappings);
				return null;
			}

			public static T Map(ICommand cmd, IDataRecord record, int index) {
				if (index >= 0 && index < MAXRECSETS) {
					int key = cmd is Script ? cmd.GetScript().GetHashCode() : cmd.GetType().GetHashCode(); // scripts are indexed by hash of script, other commands by hash of type
					var entry = GetEntry(key);
					lock (entry.Mappers) {
						if (entry.Mappers[index] == null) {
							var targets = typeof(T).GetMappableTargetProperties(true).ToDictionary(p => p.Name);
							entry.Mappings[index] = GetMappings(record, targets);
							entry.Mappers[index] = GetMapperExpression(entry.Mappings[index]).Compile();
						}
						return entry.Mappers[index](record);
					}
				}
				return null;
			}
		}

		class FieldMapping : IMapping {
			public string Source { get; private set; }
			public string Target { get; private set; }
			public Type SourceType { get; private set; }
			public Type TargetType { get; private set; }
			public int FieldIndex { get; private set; }
			public string GetMethod { get; private set; }
			public FieldMapping(string source, string target, Type sourceType, Type targetType, int fieldIndex, string getMethod) {
				Source = source;
				SourceType = sourceType;
				Target = target;
				TargetType = targetType;
				FieldIndex = fieldIndex;
				GetMethod = getMethod;
			}
		}

		static IEnumerable<IMapping> GetMappings(IDataRecord record, IDictionary<string, PropertyInfo> targets) {
			var mappings = new List<IMapping>();
			for (int fieldIndex = 0; fieldIndex < record.FieldCount; fieldIndex++) {
				string fieldName = record.GetName(fieldIndex);
				PropertyInfo targetProperty = null;
				if (targets.TryGetValue(fieldName, out targetProperty)) {
					var mapping = GetMapping(fieldName, targetProperty, record, fieldIndex);
					if (mapping != null) mappings.Add(mapping);
				}
			}
			return mappings;
		}

		static IMapping GetMapping(string fieldName, PropertyInfo targetProperty, IDataRecord record, int fieldIndex) {
			Type sourceType = record.GetFieldType(fieldIndex);
			Type targetType = targetProperty.PropertyType;
			if (!sourceType.CanMapTo(targetType)) return null;

			string getMethod = sourceType.Name;
			if (sourceType == typeof(byte[])) getMethod = "Value";
			else { // SQL Server 2008 new types
				switch (getMethod) {
					case "SqlHierarchyId":
					case "SqlGeography":
					case "SqlGeometry":
						getMethod = "Value";
						break;
				}
			}
			return new FieldMapping(fieldName, targetProperty.Name, sourceType, targetType, fieldIndex, getMethod);
		}

		static Expression<TDelegate> GetMapperExpression<TDelegate>(Type targetType, IEnumerable<IMapping> mappings) {
			var source = Expression.Parameter(typeof(IDataRecord), "source");
			var target = Expression.Parameter(targetType, "target");
			var end = Expression.Label(targetType, "end");
			var block = Expression.Block(Expression.Label(end, target));
			if (mappings.Count() > 0)
				block = Expression.Block(new[] { target },
					Expression.Assign(target, Expression.New(targetType)),
					Expression.Block(GetMappingExpressions(mappings, source, target)),
					Expression.Label(end, target));
			return Expression.Lambda<TDelegate>(block, source);
		}

		static IEnumerable<Expression> GetMappingExpressions(IEnumerable<IMapping> mappings, ParameterExpression source, ParameterExpression target) {
			foreach (FieldMapping fm in mappings) {
				var targetp = Expression.Property(target, fm.Target);
				var getter = Expression.Call(source, typeof(IDataRecord).GetMethod(string.Concat("Get", fm.GetMethod)), Expression.Constant(fm.FieldIndex));
				var assignval = Expression.Assign(targetp, Expression.Convert(getter, fm.TargetType));
				var assignnil = Expression.Assign(targetp, Expression.Convert(Expression.Constant(null), fm.TargetType));
				if (fm.TargetType.IsNullable() || fm.TargetType == typeof(string)) // check for DBNull when field is nullable
					yield return Expression.IfThenElse(Expression.Call(source, typeof(IDataRecord).GetMethod("IsDBNull"), Expression.Constant(fm.FieldIndex)), assignnil, assignval);
				else
					yield return assignval;
			}
		}

		#endregion IDataRecord Mapping
	}

	public class SqlClientConnection : Connection { public SqlClientConnection(string cs) : base(cs, new SqlClientDbProvider()) { } }
	public class OleDbConnection : Connection { public OleDbConnection(string cs) : base(cs, new OleDbDbProvider()) { } }
	public class OdbcConnection : Connection { public OdbcConnection(string cs) : base(cs, new OdbcDbProvider()) { } }
	//public class SqlServerCeConnection : Connection { public SqlServerCeConnection(string cs) : base(cs, new SqlServerCeDbProvider()) { } }

    public interface IParameter {
        string Name { get; }
        Type Type { get; }
        bool IsNullable { get; }
        DbType DbType { get; }
        ParameterDirection Direction { get; }
        int Size { get; }
        byte Precision { get; }
        byte Scale { get; }
        string UdtName { get; }
    }

	public class Parameter : IParameter {
		public string Name { get; private set; }
		public Type Type { get; private set; }
		public bool IsNullable { get; private set; }
		public DbType DbType { get; private set; }
		public ParameterDirection Direction { get; private set; }
		public int Size { get; private set; }
		public byte Precision { get; private set; }
		public byte Scale { get; private set; }
		public string UdtName { get; private set; }

		public Parameter(string name, Type type, ParameterDirection direction, ParameterAttribute attr) {
			Name = name;
			Type = type;
			Direction = direction;

			if (attr == null) {
				Size = -1; // varchar(MAX), varbinary(MAX)
				Precision = 0;
				Scale = 0;
			}
			else {
				Size = attr.Size;
				Precision = attr.Precision;
				Scale = attr.Scale;
			}

			switch (type.Name) { // handle new sql server types
				case "SqlHierarchyId":
					IsNullable = false;
					DbType = DbType.Object;
					UdtName = "HierarchyId";
					return;

				case "SqlGeography":
					IsNullable = false;
					DbType = DbType.Object;
					UdtName = "Geography";
					return;

				case "SqlGeometry":
					IsNullable = false;
					DbType = DbType.Object;
					UdtName = "Geometry";
					return;
			}

			if (type.IsNullable()) { // nullable types
				IsNullable = true;
				DbType = (DbType)Enum.Parse(typeof(DbType), type.GetGenericArguments()[0].Name);
			}
			else if (type.IsArray && type.Equals(typeof(byte[]))) { // binary types
				IsNullable = true;
				DbType = DbType.Binary;
			}
			else if (type.Equals(typeof(string))) { // strings
				IsNullable = attr == null ? false : attr.IsNullableString;
				DbType = System.Data.DbType.String;
			}
			else { // other types 
				IsNullable = false;
				DbType = (DbType)Enum.Parse(typeof(DbType), type.Name);
			}
			UdtName = null; // by default UdtName is null
		}

        public static object ToDbValue(DbType dbtype, object value, int size) {
            if (value != null) {
                if (dbtype == DbType.String && size > 0) { // trim string properties to max size
                    string str = (string)value;
                    return str.Length > size ? str.Substring(0, size) : str;
                }
                else if (dbtype == DbType.Binary && size > 0) { // trim binary properties to max size
                    byte[] bin = (byte[])value;
                    if (bin.Length > size) {
                        byte[] copy = new byte[size];
                        Array.Copy(bin, copy, size);
                        return copy;
                    }
                }
            }
            return value ?? DBNull.Value;
        }

        public static object ToValue(object value) { return value == DBNull.Value ? null : value; }
		public static object GetValue(IDbDataParameter parameter) { return ToValue(parameter.Value); }
        public static void SetValue(IDbDataParameter parameter, object value) { parameter.Value = ToDbValue(parameter.DbType, value, parameter.Size); }
	}

	public static class DynamicReader {
		static public object Materialize(ReaderExecutionContext<object> context) {
			var obj = new ExpandoObject() as IDictionary<string, object>;
			IDataRecord dr = context.Record;
			for (int i = 0; i < dr.FieldCount; i++)
				obj[dr.GetName(i)] = dr.GetValue(i);
			return obj;
		}
	}

	public class Script : ICommand {
		private string script;
		public object Parameters { get; private set; }
		public string GetScript() { return script; }

		public Script(string script, object parameters) {
			this.script = script;
			Parameters = parameters;
		}
	}

	public static class ConnectionExtensions {
		public static int MapExecute<CommandT, T>(this IConnection connection, CommandT cmd, T instance) where CommandT : class, ICommand, new() where T : class, new() {
			instance.To(cmd);
			int retval = connection.Execute(cmd);
			cmd.To(instance);
			return retval;
		}

		public static IEnumerable<ICommand> GetCommands(this Assembly assembly) {
			return assembly.GetTypes().Where(t => t.IsClass && !t.IsAbstract && t.GetInterface("ICommand") != null && t.GetConstructor(Type.EmptyTypes) != null)
				.Select(t => (ICommand)Activator.CreateInstance(t));
		}

		public static void Deploy(this ICommandDeployer deployer, IConnection connection, Assembly assembly) { deployer.Deploy(connection, assembly.GetCommands()); }
		public static void Deploy(this ICommandDeployer deployer, IConnection connection, params ICommand[] commands) { deployer.Deploy(connection, commands as IEnumerable<ICommand>); }
		public static int Execute(this IConnection connection, string script, object parameters = null) { return connection.Execute(new Script(script, parameters)); }
		public static T Execute<T>(this IConnection connection, string script, object parameters = null, ReaderCallback<T> callback = null) where T : class, new() { return connection.Execute<T>(new Script(script, parameters), callback); }
		public static T Execute<T>(this IConnection connection, ICommand cmd, ReaderCallback<T> callback = null) where T : class, new() { return connection.ExecuteReader<T>(cmd, callback).FirstOrDefault(); }
		public static T ExecuteScalar<T>(this IConnection connection, string script, object parameters = null) { return connection.ExecuteScalar<T>(new Script(script, parameters)); }
		public static IEnumerable<T> ExecuteReader<T>(this IConnection connection, string script, object parameters = null, ReaderCallback<T> callback = null) where T : class, new() { return connection.ExecuteReader<T>(new Script(script, parameters), callback); }
		public static void Execute(this IConnection connection, params ICommand[] cmds) { connection.Execute(cmds.AsEnumerable<ICommand>()); }
		public static void Execute(this IConnection connection, IEnumerable<ICommand> cmds) { foreach (var cmd in cmds) connection.Execute(cmd); }
	}
}

