using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                var mssql = OpenSqlServerConnection("Server=db;User ID=sa;Password=changeme;Database=Votes");
                var redis = OpenRedisConnection("redis").GetDatabase();

                var definition = new { vote = "", voter_id = "" };
                while (true)
                {
                    string json = redis.ListLeftPopAsync("votes").Result;
                    if (json != null)
                    {
                        var vote = JsonConvert.DeserializeAnonymousType(json, definition);
                        Console.WriteLine($"Processing vote for '{vote.vote}' by '{vote.voter_id}'");
                        UpdateVote(mssql, vote.voter_id, vote.vote);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
        }

        private static void EnsureOpen(DbConnection connection)
        {
            while (true)
            {
                try
                {
                    connection.Open();
                    return;
                }
                catch (DbException)
                {
                    Console.Error.WriteLine("Failed to connect to db - retrying");
                    Thread.Sleep(500);
                }
            }
        }

        private static SqlConnection OpenSqlServerConnection(string connectionString)
        {
            var masterConnStrBuilder = new SqlConnectionStringBuilder(connectionString)
            {
                InitialCatalog = "master"
            };
            var master = new SqlConnection(masterConnStrBuilder.ConnectionString);
            EnsureOpen(master);
            var createDb = master.CreateCommand();
            createDb.CommandText = @"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'Votes')
                                     CREATE DATABASE Votes";
            createDb.ExecuteNonQuery();

            var connection = new SqlConnection(connectionString);
            EnsureOpen(connection);

            var createTable = connection.CreateCommand();
            createTable.CommandText = @"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'votes')
                                        CREATE TABLE votes (
                                            id NVARCHAR(255) NOT NULL UNIQUE, 
                                            vote NVARCHAR(255) NOT NULL
                                        )";
            createTable.ExecuteNonQuery();
            return connection;
        }

        private static ConnectionMultiplexer OpenRedisConnection(string hostname)
        {
            // Use IP address to workaround hhttps://github.com/StackExchange/StackExchange.Redis/issues/410
            var ipAddress = GetIp(hostname);
            Console.WriteLine($"Found redis at {ipAddress}");

            while (true)
            {
                try
                {
                    return ConnectionMultiplexer.Connect(ipAddress);
                }
                catch (RedisConnectionException)
                {
                    Console.Error.WriteLine("Failed to connect to redis - retrying");
                    Thread.Sleep(1000);
                }
            }
        }

        private static string GetIp(string hostname)
            => Dns.GetHostEntryAsync(hostname)
                .Result
                .AddressList
                .First(a => a.AddressFamily == AddressFamily.InterNetwork)
                .ToString();

        private static void UpdateVote(SqlConnection connection, string voterId, string vote)
        {
            var command = connection.CreateCommand();
            try
            {
                command.CommandText = "INSERT INTO votes (id, vote) VALUES (@id, @vote)";
                command.Parameters.AddWithValue("@id", voterId);
                command.Parameters.AddWithValue("@vote", vote);
                command.ExecuteNonQuery();
            }
            catch (DbException)
            {
                command.CommandText = "UPDATE votes SET vote = @vote WHERE id = @id";
                command.ExecuteNonQuery();
            }
            finally
            {
                command.Dispose();
            }
        }
    }
}