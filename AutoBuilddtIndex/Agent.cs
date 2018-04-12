using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using kCura.Relativity.Client;
using kCura.Relativity.Client.DTOs;
using kCura.Relativity.Client.Repositories;
using kCura.Utility.CertificateGenerator;
using Relativity.API;
//This version focuses on FULL BUILDS currently THIS IS INCREMENTAL
namespace AutoBuilddtIndex
{
    [kCura.Agent.CustomAttributes.Name("Auto Build New dtIndexes")]
    [System.Runtime.InteropServices.Guid("C2C80772-204B-41A8-91C6-E6A63CB67402")]
    public class Agent : kCura.Agent.AgentBase
    {
        public override void Execute()
        {
            try
            {
                var indexObjList = new List<IndexObj>();
                using (var proxy = Helper.GetServicesManager()
                    .CreateProxy<IRSAPIClient>(ExecutionIdentity.System))
                {
                    #region Query for workspace artifact IDs and create a list

                    RaiseMessage("Proxy Initiated", 1);
                    var workspaceList = new List<int>();
                    proxy.APIOptions.WorkspaceID = -1;
                    Query<Workspace> query = new Query<Workspace>();
                    query.Condition = new WholeNumberCondition("Artifact ID", NumericConditionEnum.IsSet);
                    RaiseMessage("Querying DB", 1);
                 
                    var results = proxy.Repositories.Workspace.Query(query);

                    RaiseMessage("DB Queried, moving to foreach statements", 1);
                   
                    foreach (var workspaces in results.Results)
                    {
                        var workspace = workspaces.Artifact;
                        workspaceList.Add(workspace.ArtifactID);
                        RaiseMessage(String.Format("Added " + workspace), 1);
                    }
                    //workspaceList now has a full list of workspaces
                    //Now need to query each workspace for dtIndexes with status New

                    #endregion
                    #region Find Indexes with Status of "New" in each workspace.

                    foreach (var workspace in workspaceList)
                    {
                        IDBContext eddsDBContext = this.Helper.GetDBContext(workspace);
                        var sqlWorkspaceName = "[EDDS" + workspace + "].[EDDSDBO].[dtSearchIndex]";

                        var sqlQueryString = @"SELECT [ID],[Name] FROM " + sqlWorkspaceName + " WHERE STATUS = 'NEW'";
                        var reader = eddsDBContext.ExecuteSQLStatementAsReader(sqlQueryString);
                        while (reader.Read())
                            indexObjList.Add(new IndexObj
                            {
                                WorkspaceArtifactID = workspace,
                                SetID = reader.GetInt32(0),
                                Name = reader.GetString(1)
                            });
                        reader.Close();

                    }

                    #endregion
                    #region Create and run the insert to dtSearchIndexQueue statement for each index found

                    foreach (var dataValue in indexObjList)
                        //We have a list of indexes with status == new
                        //Use this info to create the insert statement for the queue table.
                    {
                        RaiseMessage(String.Format("Creating entry for: " + dataValue.Name), 1);
                        var time = DateTime.Now;
                        IDBContext eddsDBContext = this.Helper.GetDBContext(-1);
                        string sqlInsertString =
                            @"INSERT INTO [EDDS].[EDDSDBO].[dtSearchIndexQueue]
                              VALUES('" + dataValue.WorkspaceArtifactID + "','" + dataValue.SetID +
                            "','1015200','1015208',' ','1','"
                            + time + "','9','" +
                            time + "',NULL,'1','0','0')";

                        eddsDBContext.ExecuteNonQuerySQLStatement(sqlInsertString);
                    }
                    #endregion
                }
            }
            catch (Exception ex)
            {
                RaiseMessage(ex.ToString(), 1);
            }
        }
        public class IndexObj
        {
            public int WorkspaceArtifactID { get; set; }
            public int SetID { get; set; }
            public string Name { get; set; }
        }
        public override string Name
        {
            get { return "Auto Build NEW dtIndexes Agent"; }
        }
    }
}



