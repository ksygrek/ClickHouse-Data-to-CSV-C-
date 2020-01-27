using ClickHouse.Ado;
using System;
using System.Diagnostics;
using System.IO;

/**
 * @author ${ ksygrek }
 *
 * 
 */

namespace GetClickHouseData
{
    class Program
    {
        public static ClickHouseConnection con = null;
        public static String str = "Host=127.0.0.1;" +
            "Port=9000;" +
            "User=default;" +
            "Password=;" +
            "Database=default;" +
            "Compress=True;" +
            "CheckCompressedHash=False;" +
            "SocketTimeout=60000000;" +
            "Compressor=lz4";

        static void Main(string[] args)
        {
            Exec();
        }

        private static void Exec()
        {
            Console.WriteLine("Get ClickHouse data to CSV");
            Console.WriteLine("--------------------------");
            Console.WriteLine("Choose action:");
            Console.WriteLine("0 - Get table data");
            Console.WriteLine("1 - Exit");
            int x = int.Parse(Console.ReadLine());

            switch (x)
            {
                case 0:
                    Console.WriteLine("Enter table name: ");
                    string table = Console.ReadLine();
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    CHConnection(() => Backup(table));
                    sw.Stop();
                    Console.WriteLine($"Done. Time: { sw.Elapsed.TotalSeconds } s.");
                    Console.WriteLine("Press Enter to continue.");
                    Console.ReadLine();
                    Console.Clear();
                    Exec();
                    break;
                case 1:
                    Environment.Exit(0);
                    break;
            }
        }

        public static void CHConnection(Action myMethodName)
        {
            try
            {
                con = new ClickHouseConnection(str);
                con.Open();

                myMethodName();
            }
            catch (ClickHouseException err)
            {
                Console.WriteLine(err);
            }
            finally
            {
                if (con != null)
                {
                    con.Close();
                }
            }
        }

        public static void Backup(string table)
        {
            string command = $"SELECT * FROM { table }",
                path = $@"D:\{ DateTime.Now.Date.ToString("d") }-{ table }.txt";

            using (ClickHouseCommand comm = con.CreateCommand(command))
            {
                using (var reader = comm.ExecuteReader())
                {
                    do
                    {
                        using (StreamWriter file = new StreamWriter(path, true))
                        {
                            while (reader.Read())
                            {
                                string line = "";
                                for (var i = 0; i < reader.FieldCount; i++)
                                {
                                    var val = reader.GetValue(i);
                                    line += $"{ val },";
                                }
                                line = line.Substring(0, line.Length - 1);
                                file.WriteLine(line);
                            }
                        }
                    } while (reader.NextResult());
                }
            }
        }
    }
}
