using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Nini.Config;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Services.AdvancedAssetService;
using OpenSim.Services.AdvancedAssetService.AdvCapsServer;

namespace OpenSim.Services.AdvancedAssetService.Tests
{
    public static class AdvCapsServerTests
    {
        public static void RunTests()
        {
            Console.WriteLine("\n=================================================================");
            Console.WriteLine("RUNNING AUTOMATED INTEGRATION TESTS FOR ADVCAPSSERVER");
            Console.WriteLine("=================================================================");

            string testStoreDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "test_caps_store");
            string testIniFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "test_caps_robust.ini");

            // Clean previous environments
            if (Directory.Exists(testStoreDir))
            {
                try { Directory.Delete(testStoreDir, true); } catch {}
            }
            if (File.Exists(testIniFile))
            {
                try { File.Delete(testIniFile); } catch {}
            }

            Directory.CreateDirectory(testStoreDir);

            // 1. Create a mock texture and write it to the Pack store using PackFileManager
            UUID testTextureUuid = new UUID("00000000-0000-0000-0000-000000000123");
            byte[] testPayload = System.Text.Encoding.UTF8.GetBytes("AdvCapsServer test texture data payload of J2K image");
            
            Console.WriteLine("Writing test asset to mock storage database...");
            using (var manager = new PackFileManager(testStoreDir))
            {
                manager.StoreAssetData(testTextureUuid.ToString(), testPayload, (sbyte)AssetType.Texture, "TestTexture");
                
                // Wait up to 5 seconds for the background write thread to process and index the asset
                int retries = 50;
                while (retries > 0)
                {
                    sbyte type;
                    string name;
                    byte[] cached = manager.GetAssetData(testTextureUuid.ToString(), out type, out name, false);
                    if (cached != null && cached.Length > 0)
                    {
                        Console.WriteLine("Mock asset written and indexed successfully in L1 cache.");
                        Console.WriteLine("Waiting for SQLite batch timer to commit transaction to index.db...");
                        Thread.Sleep(2000); // Allow batch timer to flush and commit transaction
                        break;
                    }
                    Thread.Sleep(100);
                    retries--;
                }
            } // Dispose closes the database connection and flushes write threads

            // 2. Create a mock Robust.ini file for the server config
            string iniContent = $@"
[AssetService]
    StoragePath = {testStoreDir}
    VerifyOnRead = false

[CAPSTextureServer]
    Port = 8099
    Host = 127.0.0.1
    Verbose = true
";
            File.WriteAllText(testIniFile, iniContent);

            // 3. Start AdvCapsServer in a background thread
            Console.WriteLine("Spinning up AdvCapsServer in background on http://127.0.0.1:8099...");
            var serverTask = Task.Run(() => 
            {
                try
                {
                    OpenSim.Services.AdvancedAssetService.AdvCapsServer.Program.ServerMain(new string[] { testIniFile });
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Server thread error: {ex.Message}");
                }
            });

            // Wait for server initialization
            Thread.Sleep(2000);

            // 4. Run HTTP client tests
            bool allPassed = true;
            using (var client = new HttpClient())
            {
                // Test 1: Check status endpoint
                try
                {
                    Console.Write("Test 1: Status endpoint... ");
                    var response = client.GetAsync("http://127.0.0.1:8099/status").Result;
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        string body = response.Content.ReadAsStringAsync().Result;
                        if (body.Contains("AAS CAPS Texture Server"))
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine("PASSED");
                            Console.ResetColor();
                        }
                        else
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine($"FAILED (Unexpected body: {body})");
                            Console.ResetColor();
                            allPassed = false;
                        }
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"FAILED (Status code: {response.StatusCode})");
                        Console.ResetColor();
                        allPassed = false;
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"FAILED with exception: {ex.Message}");
                    Console.ResetColor();
                    allPassed = false;
                }

                // Test 2: GetTexture via path /CAPS/GetTexture/UUID
                try
                {
                    Console.Write("Test 2: Fetch texture by Path parameter... ");
                    var response = client.GetAsync($"http://127.0.0.1:8099/CAPS/GetTexture/{testTextureUuid}").Result;
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        var headers = response.Headers;
                        byte[] bodyBytes = response.Content.ReadAsByteArrayAsync().Result;
                        bool contentsMatch = System.Text.Encoding.UTF8.GetString(bodyBytes) == System.Text.Encoding.UTF8.GetString(testPayload);
                        bool isCorrectContentType = response.Content.Headers.ContentType?.ToString() == "image/x-j2c";

                        if (contentsMatch && isCorrectContentType)
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine("PASSED");
                            Console.ResetColor();
                        }
                        else
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine($"FAILED (Match: {contentsMatch}, ContentType: {response.Content.Headers.ContentType})");
                            Console.ResetColor();
                            allPassed = false;
                        }
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"FAILED (Status code: {response.StatusCode})");
                        Console.ResetColor();
                        allPassed = false;
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"FAILED with exception: {ex.Message}");
                    Console.ResetColor();
                    allPassed = false;
                }

                // Test 3: GetTexture via query variable ?texture_id=UUID
                try
                {
                    Console.Write("Test 3: Fetch texture by Query parameter... ");
                    var response = client.GetAsync($"http://127.0.0.1:8099/CAPS/GetTexture?texture_id={testTextureUuid}").Result;
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        byte[] bodyBytes = response.Content.ReadAsByteArrayAsync().Result;
                        bool contentsMatch = System.Text.Encoding.UTF8.GetString(bodyBytes) == System.Text.Encoding.UTF8.GetString(testPayload);

                        if (contentsMatch)
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine("PASSED");
                            Console.ResetColor();
                        }
                        else
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine("FAILED (content mismatch)");
                            Console.ResetColor();
                            allPassed = false;
                        }
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"FAILED (Status code: {response.StatusCode})");
                        Console.ResetColor();
                        allPassed = false;
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"FAILED with exception: {ex.Message}");
                    Console.ResetColor();
                    allPassed = false;
                }

                // Test 4: Get non-existent texture (404)
                try
                {
                    Console.Write("Test 4: Non-existent texture query (expect 404)... ");
                    UUID nonExistent = UUID.Random();
                    var response = client.GetAsync($"http://127.0.0.1:8099/CAPS/GetTexture/{nonExistent}").Result;
                    if (response.StatusCode == HttpStatusCode.NotFound)
                    {
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("PASSED");
                        Console.ResetColor();
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"FAILED (Status code: {response.StatusCode})");
                        Console.ResetColor();
                        allPassed = false;
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"FAILED with exception: {ex.Message}");
                    Console.ResetColor();
                    allPassed = false;
                }

                // Test 5: Get invalid UUID structure (400)
                try
                {
                    Console.Write("Test 5: Invalid UUID input validation (expect 400)... ");
                    var response = client.GetAsync("http://127.0.0.1:8099/CAPS/GetTexture/invalid-uuid-format-here").Result;
                    if (response.StatusCode == HttpStatusCode.BadRequest)
                    {
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("PASSED");
                        Console.ResetColor();
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"FAILED (Status code: {response.StatusCode})");
                        Console.ResetColor();
                        allPassed = false;
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"FAILED with exception: {ex.Message}");
                    Console.ResetColor();
                    allPassed = false;
                }
            }

            // 5. Clean server shutdown
            Console.WriteLine("Sending Clean Shutdown Event to AdvCapsServer...");
            OpenSim.Services.AdvancedAssetService.AdvCapsServer.Program.ExitEvent.Set();

            // Wait for server execution task to complete
            serverTask.Wait(5000);

            // 6. Clean up temporary test files
            try
            {
                // Force SQLite connection pooling to release file handles via reflection
                try
                {
                    var sqliteConnType = Type.GetType("System.Data.SQLite.SQLiteConnection, System.Data.SQLite");
                    if (sqliteConnType != null)
                    {
                        var method = sqliteConnType.GetMethod("ClearAllPools", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
                        method?.Invoke(null, null);
                    }
                }
                catch {}

                GC.Collect();
                GC.WaitForPendingFinalizers();
                Thread.Sleep(500); // Small cooldown for OS file locks to release

                if (Directory.Exists(testStoreDir))
                    Directory.Delete(testStoreDir, true);
                if (File.Exists(testIniFile))
                    File.Delete(testIniFile);
            }
            catch (Exception cleanupEx)
            {
                Console.WriteLine($"Warning: Failed to cleanup temp files: {cleanupEx.Message}");
            }

            Console.WriteLine("\n=================================================================");
            if (allPassed)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("ALL ADVCAPSSERVER INTEGRATION TESTS PASSED SUCCESSFULLY!");
                Console.ResetColor();
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("SOME ADVCAPSSERVER INTEGRATION TESTS FAILED!");
                Console.ResetColor();
                throw new Exception("AdvCapsServer automated integration tests failed.");
            }
            Console.WriteLine("=================================================================\n");
        }
    }
}
