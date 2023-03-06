using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Transactions;

namespace Knak.Data.SqlServer {
    public class SqlServerCommandDeployer : ICommandDeployer {
        public void Deploy(IConnection connection, IEnumerable<ICommand> commands) {
            using (var scope = new TransactionScope(TransactionScopeOption.Suppress)) {
                try {
                    DeployCommands(connection, commands);
                }
                catch (Exception ex) {
                    StringBuilder sb = new StringBuilder();
                    while (ex != null) {
                        sb.AppendLine(ex.Message);
                        ex = ex.InnerException;
                    }
                    throw new Exception(sb.ToString());
                }
                scope.Complete();
            }
        }
	  
        public void DeployCommands(IConnection connection, IEnumerable<ICommand> commands) {
            var server = new Server(new ServerConnection(new SqlConnection(connection.ConnectionString)));
            var database = server.Databases[server.ConnectionContext.DatabaseName];
            var dbProvider = connection.DbProvider;

            foreach (var cmd in commands) {
                string[] nameParts = connection.GetName(cmd).Replace("[", string.Empty).Replace("]", string.Empty).Split('.');
                string schemaName = nameParts.Length > 1 ? nameParts[0] : "dbo";
                string storedProcedureName = nameParts.Length > 1 ? nameParts[1] : nameParts[0];

                // create schemas
                if (!database.Schemas.Contains(schemaName)) {
                    Schema schema = new Schema(database, schemaName);
                    schema.Create();
                }

                // drop existing stored procedures
                var storedProcedure = database.StoredProcedures[storedProcedureName, schemaName];
                if (storedProcedure != null) storedProcedure.Drop();

                // create stored procedures
                storedProcedure = new StoredProcedure(database, storedProcedureName, schemaName);
                storedProcedure.TextMode = false;
				foreach (var p in cmd.GetType().GetParameters()) {
					var spp = new StoredProcedureParameter(storedProcedure, "@" + p.Name);
					if (p.DbType == DbType.String) spp.DataType = new DataType(SqlDataType.NVarChar, p.Size);
					else if (p.DbType == DbType.Decimal) spp.DataType = new DataType(SqlDataType.Decimal, p.Precision, p.Scale);
					else {
						SqlParameter sp = (SqlParameter)dbProvider.CreateDataParameter(p.Name, p.DbType, p.Direction, p.Size, p.Precision, p.Scale, p.IsNullable, p.UdtName, null);
						string typeName = Enum.GetName(typeof(SqlDbType), sp.SqlDbType);
						SqlDataType dataType = (SqlDataType)Enum.Parse(typeof(SqlDataType), typeName);
						spp.DataType = new DataType(dataType);
					}
					spp.IsOutputParameter = p.Direction != ParameterDirection.Input;
					storedProcedure.Parameters.Add(spp);
				}
                storedProcedure.TextBody = cmd.GetScript();
                storedProcedure.Create();
            }
        }
    }
}

