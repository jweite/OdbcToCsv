using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Odbc;
using CsvHelper;

namespace OdbcToCsv
{
    class Program
    {
        static void Main(string[] args)
        {
            TextWriter writer = File.CreateText(@"c:\temp\brap.csv");
            CsvHelper.CsvWriter csvWriter = new CsvWriter(writer);
            csvWriter.Configuration.Delimiter = "\t";

            string connectionString = @"Driver={SQL Server Native Client 11.0}; Server=localhost; Database=StarWars;Trusted_Connection=Yes";
            string query = "select * FROM [StarWars].[dbo].[Characters]";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                OdbcCommand command = new OdbcCommand(query, connection);

                connection.Open();
                OdbcDataReader reader = command.ExecuteReader();
                while ( reader.Read() )
                {
                    for (int i = 0; i < reader.FieldCount; ++i)
                    {

                        object col = reader[i];
                        string strCol;
                        bool isString = false;
                        switch (reader[i].GetType().Name)
                        {
                            case "String":
                                strCol = col.ToString();
                                isString = true;
                                break;
                            case "DBNull":
                                strCol = @"\N";
                                break;
                            case "DateTime":
                                strCol = col.ToString();
                                break;
                            default:
                                strCol = col.ToString();
                                break;

                        }
                        csvWriter.WriteField(strCol, isString);
                    }
                    csvWriter.NextRecord();
                }
                writer.Close();
            }
        }
    }
}
