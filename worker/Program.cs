using System;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Newtonsoft.Json;
using Npgsql;
using StackExchange.Redis;
using System.Collections.Generic;

namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                var pgsql = OpenDbConnection("Server=db;Username=postgres;Password=postgres;");
                var redisConn = OpenRedisConnection("redis");
                var redis = redisConn.GetDatabase();

                // Keep alive is not implemented in Npgsql yet. This workaround was recommended:
                // https://github.com/npgsql/npgsql/issues/1214#issuecomment-235828359
                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                while (true)
                {
                    // Slow down to prevent CPU spike, only query each 100ms
                    Thread.Sleep(100);

                    // Reconnect redis if down
                    if (redisConn == null || !redisConn.IsConnected) {
                        Console.WriteLine("Reconnecting Redis");
                        redisConn = OpenRedisConnection("redis");
                        redis = redisConn.GetDatabase();
                    }

                    string json = redis.ListLeftPopAsync("cosine_neighbors").Result;

                    if (json != null)
                    {
                        var neighborsData = JsonConvert.DeserializeObject<Dictionary<string, object>>(json);
                        Console.WriteLine($"Received neighbors data: {json}");

                        // Reconnect DB if down
                        if (!pgsql.State.Equals(System.Data.ConnectionState.Open))
                        {
                            Console.WriteLine("Reconnecting DB");
                            pgsql = OpenDbConnection("Server=db;Username=postgres;Password=postgres;");
                        }
                        else
                        {
                            // Insert or update neighbors data in PostgreSQL
                            if (UpdateNeighbors(pgsql, neighborsData))
                            {
                                Console.WriteLine("Successfully uploaded neighbors to PostgreSQL");
                            }
                            else
                            {
                                Console.WriteLine("Failed to upload neighbors to PostgreSQL");
                            }
                        }
                    }
                    else
                    {
                        keepAliveCommand.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
        }

        private static NpgsqlConnection OpenDbConnection(string connectionString)
        {
            NpgsqlConnection connection;

            while (true)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    break;
                }
                catch (SocketException)
                {
                    Console.Error.WriteLine("Waiting for db");
                    Thread.Sleep(1000);
                }
                catch (DbException)
                {
                    Console.Error.WriteLine("Waiting for db");
                    Thread.Sleep(1000);
                }
            }

            Console.Error.WriteLine("Connected to db");

            var command = connection.CreateCommand();
            
            // Create a table for neighbors data
            command.CommandText = @"CREATE TABLE IF NOT EXISTS neighbors (
                                        user_id INTEGER NOT NULL,
                                        neighbor_id INTEGER NOT NULL
                                    )";
            command.ExecuteNonQuery();

            return connection;
        }

        private static ConnectionMultiplexer OpenRedisConnection(string hostname)
        {
            // Use IP address to workaround https://github.com/StackExchange/StackExchange.Redis/issues/410
            var ipAddress = GetIp(hostname);
            Console.WriteLine($"Found redis at {ipAddress}");

            while (true)
            {
                try
                {
                    Console.Error.WriteLine("Connecting to redis");
                    return ConnectionMultiplexer.Connect(ipAddress);
                }
                catch (RedisConnectionException)
                {
                    Console.Error.WriteLine("Waiting for redis");
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

        private static bool UpdateNeighbors(NpgsqlConnection connection, Dictionary<string, object> neighborsData)
        {
            try
            {
                var command = connection.CreateCommand();
        
                int userId = Convert.ToInt32(neighborsData["user_id"]);
                var neighborIds = JsonConvert.DeserializeObject<List<int>>(neighborsData["neighbors"].ToString());
        
                // Check if neighbors already exist for the user_id
                command.CommandText = "SELECT COUNT(*) FROM neighbors WHERE user_id = @user_id";
                command.Parameters.AddWithValue("@user_id", userId);
        
                int existingCount = Convert.ToInt32(command.ExecuteScalar());
        
                if (existingCount > 0)
                {
                    Console.WriteLine($"Neighbors already exist for user_id: {userId}. Skipping insertion.");
                    return true; // No error, but also no need to insert
                }
        
                // Insert neighbors data into the table
                foreach (var neighborId in neighborIds)
                {
                    command.CommandText = "INSERT INTO neighbors (user_id, neighbor_id) VALUES (@user_id, @neighbor_id)";
                    command.Parameters.AddWithValue("@user_id", userId);
                    command.Parameters.AddWithValue("@neighbor_id", neighborId);
        
                    try
                    {
                        command.ExecuteNonQuery();
                    }
                    catch (Npgsql.PostgresException ex)
                    {
                        // If the record already exists, skip insertion
                        if (ex.SqlState == "23505") // Unique violation
                        {
                            continue;
                        }
                        else
                        {
                            Console.Error.WriteLine($"Error updating neighbors in PostgreSQL: {ex}");
                            return false;
                        }
                    }
                    finally
                    {
                        command.Parameters.Clear();
                    }
                }
        
                return true;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error updating neighbors in PostgreSQL: {ex}");
                return false;
            }
        }
    }
}