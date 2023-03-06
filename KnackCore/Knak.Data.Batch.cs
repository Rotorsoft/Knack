using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Knak.Data {
    public interface IBatchParameter : IParameter {
        IBatchItem Item { get; }
        object Value { get; set; }
        IBatchParameter Source { get; set; }
    }

    public interface IBatchItem {
        int Index { get; }
        ICommand Command { get; }
        IEnumerable<IBatchParameter> GetParameters();
        IBatchParameter this[string name] { get; }
        void MapOutput();
    }

    public interface IBatchItem<CommandT> : IBatchItem where CommandT : ICommand {
        void BindTo<TargetCommandT, PropertyT>(IBatchItem<TargetCommandT> target, Expression<Func<CommandT, PropertyT>> sourceProperty, Expression<Func<TargetCommandT, PropertyT>> targetProperty) where TargetCommandT : ICommand;
    }

	public class Batch {
        IList<IBatchItem> batch = new List<IBatchItem>(); 
		public IEnumerable<IBatchItem> GetItems() { foreach (var item in batch) yield return item; }

        public IBatchItem<CommandT> Add<CommandT, T>(CommandT cmd, T model) where CommandT : ICommand where T : class {
            var item = new BatchItem<CommandT, T>(batch.Count, cmd, model); 
            batch.Add(item);
            return item;
        }

        public IBatchItem<CommandT> Add<CommandT>(CommandT cmd) where CommandT : ICommand { 
            var item = new BatchItem<CommandT, string>(batch.Count, cmd, null);
            batch.Add(item);
            return item;
        }

 		public void Execute(IConnection connection) {
			using (var bctx = new BatchExecutionContext(connection, this)) {
				try {
					bctx.Open();
                    using (var reader = bctx.DbCommand.ExecuteReader()) {
                        if (!reader.IsClosed) { // copy output parameter values
                            while (reader.Read()) batch[reader.GetInt32(0)][reader.GetString(1)].Value = reader.GetValue(2);
                            reader.Close();
                        }
                    }
				}
				catch (Exception ex) { bctx.HandleError(ex); }
			}
            foreach (var item in batch) item.MapOutput();
		}

        class BatchExecutionContext : ExecutionContext {
            public BatchExecutionContext(IConnection connection, Batch batch) : base(connection, null) {
                CommandName = "Batch";
                Script = batch.GetScript(connection);
            }

            protected override void OpenDbCommand() {
                DbCommand = Connection.DbProvider.CreateCommand();
                DbCommand.CommandType = CommandType.Text;
                DbCommand.CommandText = Script;
                DbCommand.CommandTimeout = Connection.TimeOut;
                OpenDbConnection();
            }
        }

        class BatchParameter<CommandT> : Parameter, IBatchParameter where CommandT : ICommand {
            PropertyInfo cpi; // command property bound to parameter
            public IBatchItem Item { get; private set; }

            public BatchParameter(IBatchItem item, Parameter p) : base(p.Name, p.Type, p.Direction, null) {
                Item = item;
                cpi = typeof(CommandT).GetProperty(p.Name);
            }

            public object Value {
                get { return Parameter.ToDbValue(DbType, cpi.GetValue(Item.Command), Size); }
                set { cpi.SetValue(Item.Command, Parameter.ToValue(value)); }
            }

            public IBatchParameter Source { get; set; }
        }

        class BatchItem<CommandT, T> : IBatchItem<CommandT> where CommandT : ICommand where T : class {
            CommandT cmd;
            T model;
            public int Index { get; private set; }
            public ICommand Command { get { return cmd; } }
            public IEnumerable<IBatchParameter> GetParameters() { foreach (var p in parameters.Values) yield return p; }
            public IBatchParameter this[string name] { 
                get {
                    BatchParameter<CommandT> p = null;
                    parameters.TryGetValue(name, out p);
                    return p;
                }
            }
 
            IDictionary<string, BatchParameter<CommandT>> parameters = new Dictionary<string, BatchParameter<CommandT>>();
            public BatchItem(int index, CommandT command, T model) {
                Index = index;
                this.cmd = command;
                this.model = model;
                if (model != null) model.To(cmd);
                foreach (var p in CommandMapper.GetParameters(typeof(CommandT)))
                    parameters.Add(p.Name, new BatchParameter<CommandT>(this, p));
            }

            public void MapOutput() { if (model != null) cmd.To(model); }

            public void BindTo<TargetCommandT, PropertyT>(IBatchItem<TargetCommandT> target, Expression<Func<CommandT, PropertyT>> sourceProperty, Expression<Func<TargetCommandT, PropertyT>> targetProperty) where TargetCommandT : ICommand {
                var targetPropertyInfo = targetProperty.ToPropertyInfo();
                if (!targetPropertyInfo.CanWrite) throw new ArgumentException("Target property not writeable.", "targetProperty");
                var sourcePropertyInfo = sourceProperty.ToPropertyInfo();
                if (!sourcePropertyInfo.CanRead) throw new ArgumentException("Source property not readable.", "sourceProperty");
                target[targetPropertyInfo.Name].Source = this[sourcePropertyInfo.Name];
            }
        }
	}

	public static class SqlClientBatchExtensions {
        static readonly string BatchHeader = "declare @output table(commandIndex int, parameterName varchar(100), parameterValue sql_variant)\n";
        static readonly string BatchFooter = "select commandIndex, parameterName, parameterValue from @output";

		public static string GetScript(this Batch batch, IConnection connection) {
			var sb = new StringBuilder();
			bool hasOutput = false;
			foreach (var item in batch.GetItems())
                if (ScriptExecute(sb, connection, item)) hasOutput = true;
			if (hasOutput) sb.Insert(0, BatchHeader).AppendLine().Append(BatchFooter);
			return sb.ToString();
		}

		static bool ScriptExecute(StringBuilder sb, IConnection connection, IBatchItem item) {
			var declarations = new StringBuilder();
			var parameters = new StringBuilder();
			var outputs = new StringBuilder();
			foreach (var p in item.GetParameters()) {
				if (parameters.Length > 0) parameters.Append(", ");
				if (p.Direction == ParameterDirection.Input) parameters.Append(ScriptParameterValue(p));
				else { // output parameters
					ScriptParameterDeclaration(declarations, connection.DbProvider, p);
					parameters.Append(ScriptParameterName(p)).Append(" OUTPUT");
					outputs.AppendLine(string.Format("insert into @output values({0}, '{1}', {2})", item.Index, p.Name, ScriptParameterName(p)));
				}
			}
			sb.Append(declarations);
			sb.Append("EXEC ").Append(connection.GetName(item.Command)).Append(" ").Append(parameters).AppendLine(";");
			sb.Append(outputs);
			return outputs.Length > 0;
		}

		static void ScriptParameterDeclaration(StringBuilder sb, IDbProvider provider, IBatchParameter p) {
			SqlParameter sqlParameter = (SqlParameter)provider.CreateDataParameter(p.Name, p.DbType, p.Direction, p.Size, p.Precision, p.Scale, p.IsNullable, p.UdtName, null);
			string name = ScriptParameterName(p);
			sb.AppendLine().Append("declare ").Append(name).Append(" ").AppendLine(sqlParameter.SqlDbType.ToString());
			if (p.Direction == ParameterDirection.InputOutput) sb.Append("set ").Append(name).Append(" = ").AppendLine(ScriptParameterValue(p));
		}

		static string ScriptParameterName(IBatchParameter p) { return "@" + p.Name + p.Item.Index; }

		static string ScriptParameterValue(IBatchParameter p) {
            if (p.Source != null) return ScriptParameterName(p.Source); // use source parameter name instead of value

			object value = p.Value;
			if (value == DBNull.Value || value == null) {
				if (p.IsNullable) return "null";
				throw new Exception(string.Format("Not nullable command parameter {0} with null value.", p.Name));
			}
			switch (p.DbType) {
				case DbType.Boolean:    return FormatBoolean((bool)value);
				case DbType.DateTime:	return FormatDateTime((DateTime)value);
				case DbType.Guid:	    return FormatString(value.ToString());
				case DbType.String:	    return FormatString(value.ToString());
				default:			    return value.ToString();
			}
		}

		static string FormatString(string value) { return string.Format("'{0}'", value.Replace("'", "''")); }
		static string FormatDate(DateTime value) { return string.Format("'{0:0000}{1:00}{2:00}'", value.Year, value.Month, value.Day); }
		static string FormatTime(DateTime value) { return string.Format("'{0:00}:{1:00}'", value.Hour, value.Minute); }
		static string FormatDateTime(DateTime value) { return string.Format("'{0:0000}{1:00}{2:00} {3:00}:{4:00}'", value.Year, value.Month, value.Day, value.Hour, value.Minute); }
		static string FormatBoolean(bool value) { return value ? "1" : "0"; }
	}
}

