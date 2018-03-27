using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Odbc;
using CsvHelper;
using CommandLine;
using CommandLine.Text;

namespace OdbcToCsv
{
    class Program
    {
        static void Main(string[] args)
        {
            Options options;

            options = new Options();
            CommandLine.Parser.Default.ParseArguments(args, options);
            if (options.OutputFileName == null || options.ConnectionString == null || options.Query == null)
            {
                Console.WriteLine(options.GetUsage());
                return;
            }

            DateTime startTime = DateTime.Now;

            TextWriter writer = File.CreateText(options.OutputFileName);
            CsvHelper.CsvWriter csvWriter = new CsvWriter(writer);
            csvWriter.Configuration.Delimiter = options.FieldDelimiter == "\\t"? "\t" : options.FieldDelimiter == null ? "," : options.FieldDelimiter;

            string nullPlaceholder = options.NullPlaceholder == null ? "" : options.NullPlaceholder;

            using (OdbcConnection connection = new OdbcConnection(options.ConnectionString))
            {
                OdbcCommand command = new OdbcCommand(options.Query, connection);

                connection.Open();
                OdbcDataReader reader = command.ExecuteReader();
                int rowsImported = 0;
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
                                strCol = nullPlaceholder;
                                break;
                            case "DateTime":
                                strCol = ((DateTime)col).ToString("yyyy-MM-dd hh:mm:ss");
                                isString = true;
                                break;
                            default:
                                strCol = col.ToString();
                                break;

                        }
                        csvWriter.WriteField(strCol, isString);
                    }
                    csvWriter.NextRecord();
                    ++rowsImported;
                    if (rowsImported % 1000 == 0)
                    {
                        Console.WriteLine("{0} rows imported...", rowsImported);
                    }
                }
                writer.Close();
                Console.WriteLine("{0} rows imported.", rowsImported);
                Console.WriteLine("Started {0}, Finished {1}.  RunTime {2}", startTime.ToString(), DateTime.Now.ToString(), DateTime.Now.Subtract(startTime).ToString() );
            }
        }
    }

    class Options
    {
        [Option('o', "file", Required = true, HelpText = "Path/filename to export to.")]
        public string OutputFileName { get; set; }

        [Option('c', "connectionString", Required = true, HelpText = "Connection string for DB to export from.")]
        public string ConnectionString { get; set; }

        [Option('q', "query", Required = true, HelpText = "Query to export data with.")]
        public string Query { get; set; }

        [Option('f', "fieldDelimiter", Required = false, HelpText = "string to put between fields.")]
        public string FieldDelimiter { get; set; }

        [Option('n', "nullPlaceholder", Required = false, HelpText = "Text to output for null columns.")]
        public string NullPlaceholder { get; set; }

        public string GetUsage()
        {
            return HelpText.AutoBuild(this,
              (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }

    }

}
