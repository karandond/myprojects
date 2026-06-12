
using CaioCs;
using Fleck;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.IO;
using AioCardService;
public class Program
{
    private static Caio aio = new Caio();
    private static readonly object aioLock = new();
    private static readonly object aio001Lock = new(); // AIO001 lock

    private static short id = 0;
    private static short aio001_id = 0;
    //private static short ai_channels = 32;
    private static short device0Channels = 32;
    private static short device1Channels = 4;
    private static CancellationTokenSource cancelSource = new CancellationTokenSource();
    private static List<IWebSocketConnection> allSockets = new List<IWebSocketConnection>();
    private static bool hasExited = false;
    private static bool running = false;
    private static volatile bool streamPaused = false;
    // ===== WebSocket client lists =====
    private static readonly List<IWebSocketConnection> streamClients = new();
    private static readonly List<IWebSocketConnection> sampleClients = new();
    private static readonly List<IWebSocketConnection> streamClientsAio001 = new();
    private static readonly List<IWebSocketConnection> sampleClientsAio001 = new();
    // Device names from config
    private static string device0Name = "AIO000";
    private static string device1Name = "AIO001";
    private static short device1DataChannel = 0;
    private static short device0Range = 51;
    private static float device0SamplingClockPeriod = 1000f;
    private static short device0SamplingClock = 0;
    private static short device1Range = 51;
    private static float device1SamplingClockPeriod = 1000f;
    private static short device1SamplingClock = 0;
    private static float device1ConversionSpeed = 1f;
    private static float device0ConversionSpeed = 1f;
    private static short device0TriggerMode = 0;
    private static short device1TriggerMode = 0;

    private static int[] trgConfigChannels = [0, 26];
    private static int[] VVTDZChannels = [27, 28, 29];
    public static void Main(string[] args)
    {
        LoadDeviceNamesFromConfig();
        int ret;
        int ret1;
        string error_string;

        // ==================== Initialize AIO000 ====================
        ret = aio.Init(device0Name, out id);
        aio.GetErrorString(ret, out error_string);
        //Console.WriteLine($"Init {device0Name}: {ret} - {error_string}");
        Logger.AioLog($"Init {device0Name}: {ret} - {error_string}", 0, $"{device0Name}.Main");
        if (ret != 0)
        {
            //Console.WriteLine($"Failed to initialize {device0Name}. Exiting.");
            Logger.AioLog($"Failed to initialize {device0Name}. Exiting.", 1, $"{device0Name}.Main");
            return;
        }
        lock (aioLock)
        {
            ret = aio.ResetDevice(id);
            aio.GetErrorString(ret, out error_string);
            //Console.WriteLine($"ResetDevice: {ret} - {error_string}");
            Logger.AioLog($"ResetDevice: {ret} - {error_string}", 0, $"{device0Name}.Main");

            ret = aio.SetAiRangeAll(id, device0Range);//change on site
            aio.GetErrorString(ret, out error_string);
            //Console.WriteLine($"SetAiRangeAll: {ret} - {error_string}");
            Logger.AioLog($"SetAiRangeAll: {ret} - {error_string}", 0, $"{device0Name}.Main");

            ret = aio.SetAiInputMethod(id, 0);
            aio.GetErrorString(ret, out error_string);
            //Console.WriteLine($"SetAiInputMethod: {ret} - {error_string}");
            Logger.AioLog($"SetAiInputMethod: {ret} - {error_string}", 0, $"{device0Name}.Main");

            ret = aio.SetAiChannels(id, device0Channels);
            aio.GetErrorString(ret, out error_string);
            //Console.WriteLine($"SetAiChannels: {ret} - {error_string}");
            Logger.AioLog($"SetAiChannels: {ret} - {error_string}", 0, $"{device0Name}.Main");
        }

        // ==================== Initialize AIO001 ====================
        ret1 = aio.Init(device1Name, out aio001_id);
        aio.GetErrorString(ret1, out error_string);
        //Console.WriteLine($"Init {device1Name}: {ret1} - {error_string}");
        Logger.AioLog($"Init {device1Name}: {ret} - {error_string}", 0, $"{device1Name}.Main");
        if (ret1 == 0)
        {
            lock (aio001Lock)
            {
                ret1 = aio.ResetDevice(aio001_id);
                aio.GetErrorString(ret1, out error_string);
                //Console.WriteLine($"[{device1Name}] ResetDevice: {ret1} - {error_string}");
                Logger.AioLog($"ResetDevice: {ret} - {error_string}", 0, $"{device1Name}.Main");

                ret1 = aio.SetAiRangeAll(aio001_id, device1Range);//change on site
                aio.GetErrorString(ret1, out error_string);
                //Console.WriteLine($"[{device1Name}] SetAiRangeAll: {ret1} - {error_string}");
                Logger.AioLog($"SetAiRangeAll: {ret} - {error_string}", 0, $"{device1Name}.Main");

                ret1= aio.SetAiInputMethod(aio001_id, 0);
                aio.GetErrorString(ret1, out error_string);
                //Console.WriteLine($"[{device1Name}] SetAiInputMethod: {ret1} - {error_string}");
                Logger.AioLog($"SetAiInputMethod: {ret} - {error_string}", 0, $"{device1Name}.Main");

                ret1 = aio.SetAiChannelSequence(aio001_id,0,device1DataChannel);
                aio.GetErrorString(ret1, out error_string);
                //Console.WriteLine($"[{device1Name}] SetAiChannelSequence: {ret1} - {error_string}");
                Logger.AioLog($"SetAiChannelSequence: {ret} - {error_string}", 0, $"{device1Name}.Main");
                ret1 = aio.SetAiChannels(aio001_id, device1Channels);
                aio.GetErrorString(ret1, out error_string);
                //Console.WriteLine($"[{device1Name}] SetAiChannels: {ret1} - {error_string}");
                Logger.AioLog($"SetAiChannels: {ret} - {error_string}", 0, $"{device1Name}.Main");
            }
        }
        else
        {
            //Console.WriteLine($"[WARN] {device1Name} not initialized properly.");
            Logger.AioLog($"[WARN] {device1Name} not initialized properly.", 1, $"{device1Name}.Main");
            return;
        }

        // ==================== WebSocket Server AIO000 ====================
        var server = new WebSocketServer("ws://0.0.0.0:8181");
        server.Start(socket =>
        {
            //socket.OnOpen = () => allSockets.Add(socket);
            // socket.OnClose = () => allSockets.Remove(socket);
            //socket.OnError = ex => Console.WriteLine("WebSocket Error: " + ex.Message);
            var path = socket.ConnectionInfo.Path.Trim('/').ToLower();

            if (path == "multiai")
            {
                socket.OnOpen = () =>
                {
                    lock (streamClients)
                    {
                        streamClients.Add(socket);
                        //Console.WriteLine($"[INFO] /multiai connected ({streamClients.Count} total)");
                        Logger.AioLog($"[INFO] /multiai connected ({streamClients.Count} total)", 0, $"{device0Name}.Main");

                    }
                };
                socket.OnClose = () => { lock (streamClients) streamClients.Remove(socket); };
                socket.OnError = ex => Console.WriteLine($"[MultiAI Error] {ex.Message}");
                socket.OnMessage = message =>
                {
                    if (message.StartsWith("shutdown"))
                    {
                        //Console.WriteLine("Shutdown command received.");
                        Logger.AioLog("Shutdown command received.", 0, $"{device0Name}.Main");
                        CleanUpAndExit();
                    }
                    //Console.WriteLine("Message received: " + message);
                    Logger.AioLog("Message received: " + message, 0, $"{device0Name}.Main");
                    var parts = message.Trim().Split(' ');
                    string cmd = parts[0].ToLower();


                    if (cmd == "shutdown")
                    {
                        //Console.WriteLine("Shutdown command received.");
                        Logger.AioLog("Shutdown command received.", 0, $"{device0Name}.Main");
                        CleanUpAndExit();
                        cancelSource.Cancel();

                    }
                };

            }
            else if (path == "sampling")
            {
                socket.OnOpen = () =>
                {
                    lock (sampleClients)
                    {
                        sampleClients.Add(socket);
                        //Console.WriteLine($"[INFO] /sampling connected ({sampleClients.Count} total)");
                        Logger.AioLog($"[INFO] /sampling connected ({sampleClients.Count} total)", 0, $"{device0Name}.Main");
                    }
                };
                socket.OnClose = () => { lock (sampleClients) sampleClients.Remove(socket); Console.WriteLine($"[{device0Name}][INFO] /sampling disconnected"); };
                socket.OnError = ex => Console.WriteLine($"[Sampling Error] {ex.Message}");
                Console.WriteLine("about to start");
                socket.OnMessage = msg =>
                {
                    if (msg.StartsWith("get_samples"))
                    {
                        //Console.WriteLine("get_samples request received");
                        Logger.AioLog("get_samples request received", 0, $"{device0Name}.Main");
                        var parts = msg.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length < 3)
                        {
                            socket.Send("{\"error\":\"Usage: get_samples <channels> <samples>\"}");
                            return;
                        }

                        short channels = short.Parse(parts[1]);
                        int samples = int.Parse(parts[2]);
                        string channelListRaw = parts.Length >= 4 ? parts[3] : null;
                        int[] requestedChannels = channelListRaw != null ? ParseChannelArray(channelListRaw.AsSpan()) : Array.Empty<int>();
                        // Parse optional samplingClockPeriod and conversionSpeed
                        float samplingClockPeriod = parts.Length >= 5 && float.TryParse(parts[4], out float scp) ? scp : device0SamplingClockPeriod;
                        float conversionSpeed = parts.Length >= 6 && float.TryParse(parts[5], out float cs) ? cs : device0ConversionSpeed;
                        //Console.WriteLine($"[{device0Name}][Sampling] Request: count={channels}, samples={samples}, array=[{(channelListRaw ?? "NONE")}] (parsed {requestedChannels.Length})");
                        Logger.AioLog($"[{device0Name}][Sampling] Request: count={channels}, samples={samples}, array=[{(channelListRaw ?? "NONE")}] (parsed {requestedChannels.Length}), samplingClockPeriod={samplingClockPeriod}, conversionSpeed={conversionSpeed}", 0, $"{device0Name}.Main");
                        //Console.WriteLine($" channel array recieved : {parts[3]}");

                        //int[] channelArr = parts[3]
                        //    .Split(',')                      // Split the string by commas
                        //    .Select(int.Parse)              // Convert each part to int
                        //    .ToArray();

                        ThreadPool.QueueUserWorkItem(_ =>
                        {
                            try
                            {
                                lock (aioLock)
                                {
                                    streamPaused = true;
                                    double[][] data = AcquireSamples(channels, samples, requestedChannels, samplingClockPeriod, conversionSpeed);
                                    streamPaused = false;

                                    var payload = JsonSerializer.Serialize(new
                                    {
                                        type = "samples",
                                        channels,
                                        samples,
                                        data
                                    });

                                    socket.Send(payload);
                                }
                            }
                            catch (Exception ex)
                            {
                                socket.Send($"{{\"error\":\"{ex.Message}\"}}");
                                Logger.AioLog("Error in get_samples: " + ex.Message, 1, $"{device0Name}.Main");
                                streamPaused = false;
                            }
                        });
                    }
                    else if (msg.StartsWith("shutdown"))
                    {
                        //Console.WriteLine("Shutdown Command Received");
                        Logger.AioLog("Shutdown Command Received", 0, $"{device0Name}.Main");
                        CleanUpAndExit();
                    }
                };
            }
            else
            {
                socket.Send("{\"error\":\"Unknown endpoint. Use /multiai or /sampling\"}");
                socket.Close();
            }
        });

        // ==================== WebSocket Server AIO001 ====================
        var serverAio001 = new WebSocketServer("ws://0.0.0.0:8282");
        serverAio001.Start(socket =>
        {
            var path = socket.ConnectionInfo.Path.Trim('/').ToLower();

            if (path == "multiai")
            {
                socket.OnOpen = () =>
                {
                    lock (streamClientsAio001)
                    {
                        streamClientsAio001.Add(socket);
                        //Console.WriteLine($"[AIO001][INFO] /multiai connected ({streamClientsAio001.Count} total)");
                        Logger.AioLog($"[AIO001][INFO] /multiai connected ({streamClientsAio001.Count} total)", 0, $"{device1Name}.Main");
                    }
                };

                socket.OnClose = () =>
                {
                    lock (streamClientsAio001)
                    {
                        streamClientsAio001.Remove(socket);
                        //Console.WriteLine($"[{device1Name}][INFO] /multiai disconnected");
                        Logger.AioLog($"[{device1Name}][INFO] /multiai disconnected", 0, $"{device1Name}.Main");
                    }
                };

                socket.OnError = ex =>
                {
                    //Console.WriteLine($"[{device1Name}][ERROR] WebSocket Error: {ex.Message}");
                    Logger.AioLog($"[{device1Name}][ERROR] WebSocket Error: {ex.Message}",1, $"{device1Name}.Main");
                };

                socket.OnMessage = msg =>
                {
                    if (msg.StartsWith("shutdown", StringComparison.OrdinalIgnoreCase))
                    {
                        //Console.WriteLine($"[{device1Name}][CMD] Shutdown command received.");
                        Logger.AioLog($"[{device1Name}][CMD] Shutdown command received.",0,$"{device1Name}.Main");
                        CleanUpAndExit();
                    }

                    //Console.WriteLine("Message received: " + msg);
                    Logger.AioLog("Message received: " + msg, 0, $"{device1Name}.Main");
                    var parts = msg.Trim().Split(' ');
                    string cmd = parts[0].ToLower();


                    if (cmd == "shutdown")
                    {
                        //Console.WriteLine("Shutdown command received.");
                        Logger.AioLog("Shutdown command received.", 0, $"{device1Name}.Main");
                        CleanUpAndExit();
                        cancelSource.Cancel();

                    }
                };
            }
            else if (path == "sampling")
            {
                socket.OnOpen = () =>
                {
                    lock(sampleClientsAio001)
                    {
                        sampleClientsAio001.Add(socket);
                        //Console.WriteLine($"[{device1Name}][INFO] /sampling connected");
                        Logger.AioLog($"[{device1Name}][INFO] /sampling connected", 0, $"{device1Name}.Main");
                    }
                  
                };

                socket.OnClose = () =>
                {
                    lock (sampleClientsAio001) sampleClientsAio001.Remove(socket);
                    //Console.WriteLine($"[{device1Name}][INFO] /sampling disconnected");
                    Logger.AioLog($"[{device1Name}][INFO] /sampling disconnected", 0, $"{device1Name}.Main");
                };

                socket.OnError = ex => Console.WriteLine($"[{device1Name}][ERROR][Sampling] {ex.Message}");

                socket.OnMessage = msg =>
                {
                    if (msg.StartsWith("get_samples"))
                    {
                        //Console.WriteLine($"[{device1Name}][INFO] get_samples request received");
                        Logger.AioLog($"[{device1Name}][INFO] get_samples request received", 0, $"{device1Name}.Main");
                        var parts = msg.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length < 3)
                        {
                            socket.Send("{\"error\":\"Usage: get_samples <channels> <samples>\"}");
                            return;
                        }

                        short channels = short.Parse(parts[1]);
                        int samples = int.Parse(parts[2]);

                        ThreadPool.QueueUserWorkItem(_ =>
                        {
                            try
                            {
                                lock (aio001Lock)
                                {
                                    streamPaused = true;
                                    double[][] data = AcquireSamplesAio001(channels, samples);
                                    streamPaused = false;
                                    var payload = JsonSerializer.Serialize(new
                                    {
                                        type = "samples",
                                        channels,
                                        samples,
                                        data
                                    });
                                    socket.Send(payload);
                                }
                            }
                            catch (Exception ex)
                            {
                                socket.Send($"{{\"error\":\"{ex.Message}\"}}");
                                Logger.AioLog("Error in get_samples: " + ex.Message, 1, $"{device1Name}.Main");
                                streamPaused = false;
                            }
                        });
                    }
                    else if (msg.StartsWith("shutdown", StringComparison.OrdinalIgnoreCase))
                    {
                        //Console.WriteLine($"[{device1Name}][CMD] Shutdown command received (sampling).");
                        Logger.AioLog($"[{device1Name}][CMD] Shutdown command received (sampling).",0,$"{device1Name}.Main");
                        CleanUpAndExit();
                    }
                };
            }
            else
            {
                //Console.WriteLine($"[{device1Name}][WARN] Invalid endpoint access: {path}");
                    Logger.AioLog($"[{device1Name}][WARN] Invalid endpoint access: {path}", 1, $"{device1Name}.Main");
                socket.Send($"{{\"error\":\"[{device1Name}] Unknown endpoint. Use /multiai or /sampling.\"}}");
                socket.Close();
            }
        });

        //Console.WriteLine("[SERVER READY]");
        //Console.WriteLine($"  ws://localhost:8181/multiai   -> continuous streaming ({device0Name})");
        //Console.WriteLine($"  ws://localhost:8181/sampling  -> on-demand get_samples ({device0Name})");
        //Console.WriteLine($"  ws://localhost:8282/multiai   -> continuous streaming ({device1Name})");
        //Console.WriteLine($"  ws://localhost:8282/sampling  -> on-demand get_samples ({device1Name})");
       // Console.WriteLine("\n[INFO] Waiting for 'shutdown' command via WebSocket...\n");
        Logger.AioLog("[SERVER READY]", 0, "Main");
        Logger.AioLog($"  ws://localhost:8181/multiai   -> continuous streaming ({device0Name})", 0, "Main");
        Logger.AioLog($"  ws://localhost:8181/sampling  -> on-demand get_samples ({device0Name})", 0, "Main");
        Logger.AioLog($"  ws://localhost:8282/multiai   -> continuous streaming ({device1Name})", 0, "Main");
        Logger.AioLog($"  ws://localhost:8282/sampling  -> on-demand get_samples ({device1Name})", 0, "Main");


        new Thread(StreamLoop) { IsBackground = true }.Start();
        new Thread(StreamLoopAio001) { IsBackground = true }.Start();

        // Keep main thread alive
        while (true)
        {
            Thread.Sleep(1000);
        }
    }

    private static int[] ParseChannelArray(ReadOnlySpan<char> span)
    {
        if (span.IsEmpty) return Array.Empty<int>();

        // Count numbers
        int count = 1;
        for (int i = 0; i < span.Length; i++)
            if (span[i] == ',') count++;

        int[] result = new int[count];
        int idx = 0;
        int current = 0;
        bool hasDigit = false;

        for (int i = 0; i < span.Length; i++)
        {
            char c = span[i];
            if (c >= '0' && c <= '9')
            {
                current = current * 10 + (c - '0');
                hasDigit = true;
            }
            else if (c == ',')
            {
                if (hasDigit)
                {
                    result[idx++] = current;
                    current = 0;
                    hasDigit = false;
                }
            }
        }
        if (hasDigit && idx < result.Length)
            result[idx++] = current;
        return result;
    }

    // ==================== Stream Loop (AIO000) ====================
    private static void StreamLoop()
    {
        float[] ai_data = new float[device0Channels];
        int ret;
        string err;
        short seq = 0;
        //for (int i = 0; i < device0Channels; i++)
        //{
        //    ret = aio.SetAiChannelSequence(id, seq, (short)i);
        //    if (ret != 0)
        //    {
        //        aio.GetErrorString(ret, out err);
        //        Console.WriteLine("SetAiChannelSequence: " + err);
        //    }
        //    seq++;
        //}
        running = true;
        int streamCheckCounter = 0;
        //var lastTime = DateTime.Now;
        while (running)
        {
            if (streamPaused)
            {
                Thread.Sleep(100);
                streamCheckCounter += 1;
                continue;
            }
            if (streamCheckCounter > 0) // Every ~1s when paused, ensure channels are configured
            {
                seq = 0;
                for (int i = 0; i < device0Channels; i++)
                {
                    ret = aio.SetAiChannelSequence(id, seq, (short)i);
                    if (ret != 0)
                    {
                        aio.GetErrorString(ret, out err);
                        //Console.WriteLine("SetAiChannelSequence: " + err);
                        //Logger.AioLog("SetAiChannelSequence: " + err, 1, $"StreamLoop");
                    }
                    seq++;
                }
                ret = aio.SetAiChannels(id, device0Channels);
                if (ret != 0)
                {
                    aio.GetErrorString(ret, out err);
                    Logger.AioLog("SetAiChannels: " + err, 1, $"StreamLoop");
                }
                streamCheckCounter = 0;
            }

            try
            {
            lock (aioLock)
            {
                    
                        ret = aio.MultiAiEx(id, device0Channels, ai_data);
                    //Logger.AioLog($"MultiAiEx called at {DateTime.Now:HH:mm:ss.fff} within duration {(DateTime.Now - lastTime).TotalMilliseconds} ms", 0, $"StreamLoop");
                    //lastTime = DateTime.Now;

            }

            aio.GetErrorString(ret, out err);
            if (ret != 0)
            {
                //Console.WriteLine($"MultiAiEx Error: {ret} - {err}");
                    Logger.AioLog($"MultiAiEx Error: {ret} - {err}", 1, $"StreamLoop");
                Thread.Sleep(200);
                continue;
            }

            var dataDict = new Dictionary<string, float>();
                    for (int i = 0; i < device0Channels; i++)
            {
                dataDict[$"channel_{i}"] = ai_data[i];
            }

            //Prepare JSON message with type tag
            var payload = JsonSerializer.Serialize(new
            {
                type = "stream",
                data = dataDict
            });
            //var payload = JsonSerializer.Serialize(dataDict);

            lock (streamClients)
            {
                foreach (var c in streamClients.ToArray())
                {
                    if (c.IsAvailable) c.Send(payload);
                    else streamClients.Remove(c);
                }
            }

            Thread.Sleep(100); // Adjust update rate
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"[ERROR] StreamLoop exception: {ex.Message}");
                Logger.AioLog($"[ERROR] StreamLoop exception: {ex.Message}", 1, $"StreamLoop");
                Thread.Sleep(500);
            }
        }
    }
        
       // ==================== Stream Loop (AIO001) ====================
    private static void StreamLoopAio001()
    {
        float[] ai_data = new float[device1Channels];
        int ret;
        string err;

        while (true)
        {
            if ((streamPaused))
            {
                Thread.Sleep(100);
                continue;
            }
            try
            {
                lock (aio001Lock)
                {
                    ret = aio.MultiAiEx(aio001_id, 1, ai_data);
                }

                aio.GetErrorString(ret, out err);
                if (ret != 0)
                {
                    //Console.WriteLine($"[AIO001] MultiAiEx Error: {ret} - {err}");
                        Logger.AioLog($"[AIO001] MultiAiEx Error: {ret} - {err}", 1, $"StreamLoopAio001");
                    Thread.Sleep(200);
                    continue;
                }

                var dataDict = new Dictionary<string, float>();
                for (int i = 0; i < device1Channels; i++)
                    dataDict[$"channel_{i}"] = ai_data[i];

                var payload = JsonSerializer.Serialize(new { type = "stream", data = dataDict });

                lock (streamClientsAio001)
                {
                    foreach (var c in streamClientsAio001.ToArray())
                    {
                        if (c.IsAvailable) c.Send(payload);
                        else streamClientsAio001.Remove(c);
                    }
                }

                Thread.Sleep(100);
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"[ERROR][{device1Name}] StreamLoop exception: {ex.Message}");
                Logger.AioLog($"[ERROR][{device1Name}] StreamLoop exception: {ex.Message}", 1, $"StreamLoopAio001");
                Thread.Sleep(500);
            }
        }
    }

    // ==================== Cleanup ====================
    private static void CleanUpAndExit()
    {
        if (hasExited) return;
        hasExited = true;

        //Console.WriteLine("[CLEANUP] Releasing AIO resources and shutting down WebSocket server.");
        Logger.AioLog("[CLEANUP] Releasing AIO resources and shutting down WebSocket server.", 1, "CleanUpAndExit");

        int ret;
        string error_string;

        ret = aio.Exit(id);
        aio.GetErrorString(ret, out error_string);
        //Console.WriteLine($"Exit {device0Name}: {ret} - {error_string}");
        

        ret = aio.Exit(aio001_id);
        aio.GetErrorString(ret, out error_string);
        //Console.WriteLine($"Exit {device1Name}: {ret} - {error_string}");

        foreach (var socket in allSockets.ToArray())
        {
            try { socket.Close(); } catch { /* Ignore */ }
        }

        //Console.WriteLine("[CLEANUP] Server shut down cleanly.");
        Logger.AioLog("[CLEANUP] Server shut down cleanly.", 0, "CleanUpAndExit");
        Environment.Exit(1);
    }

    // ==================== Acquire Samples (AIO000) ====================
    private static double[][] AcquireSamples(short channels, int stopTimes, int[] requestedChannels, float samplingClockPeriod, float conversionSpeed)
    {
        const int MAX_BUFFER_FLOATS = 1024 * 1024;
        int ret;
        string error;
        int samplesperchannel = 0;
        short externalClockEdge = 1;  // 0: Falling, 1: Rising (only for external)
        // Pause continuous acquisition (if needed)
        // For safety you could have a flag controlling MultiAiEx loop

        // Configure only required channels
        try
        {
            short seq = 0;
            foreach (var ch in requestedChannels)
            {
                if (ch < 0 || ch >= device0Channels) {
                    Logger.AioLog($"Invalid channel index requested: {ch}", 1, $"{device0Name}.AcquireSamples");
                    throw new Exception($"Invalid channel index requested: {ch}"); }
                ret = aio.SetAiChannelSequence(id, seq, (short)ch);
                if (ret != 0)
                {
                    aio.GetErrorString(ret, out error);
                    //Console.WriteLine("SetAiChannelSequence: " + error);
                    //Logger.AioLog("SetAiChannelSequence: " + error, 1, $"{device0Name}.AcquireSamples");
                }
                seq++;
            }
            ret = aio.SetAiChannels(id, channels);
            aio.GetErrorString(ret, out error);
            if (ret != 0) {
                Logger.AioLog("SetAiChannels: " + error, 1, $"{device0Name}.AcquireSamples");
                throw new Exception("SetAiChannels: " + error); }
            //float[] aioData = new float[1];
            //if (trgConfigChannels.Contains(requestedChannels[0]))
            //{
            //    ret = aio.GetAiChannelSequence(id, 0, out short trgCh);
            //    Logger.AioLog($"Trigger channel for start level check: {trgCh}", 0, $"{device0Name}.AcquireSamples");
            //    var counter = 0;
            //    while (aioData[0] < 1)
            //    {                   
            //        aio.MultiAiEx(id, 1, aioData);
            //        Logger.AioLog($"Waiting for trigger channel {requestedChannels[0]} val  :{aioData[0]}", 0, $"{device0Name}.AcquireSamples");
            //        counter++;
            //        if (counter == 50)
            //        {
            //            break;
            //        }
            //    }
            //}
            ret = aio.SetAiTransferMode(id, 0);
            aio.GetErrorString(ret, out error);
            //Console.WriteLine($"SetAiTransferMode: {ret} - {error}");
            Logger.AioLog($"SetAiTransferMode: {ret} - {error}", 0, $"{device0Name}.AcquireSamples");
            ret = aio.SetAiMemoryType(id, 0);
            aio.GetErrorString(ret, out error);
            //Console.WriteLine($"SetAiMemoryType: {ret} - {error}");
            Logger.AioLog($"SetAiMemoryType: {ret} - {error}", 0, $"{device0Name}.AcquireSamples");
            var triggerMode=device0TriggerMode;
            if (requestedChannels == VVTDZChannels)
            {
                triggerMode = 0;
            }
            
            ret=aio.SetAiStartTrigger(id, triggerMode);
            aio.GetErrorString(ret, out error);
            Logger.AioLog($"SetAiStartTrigger: {ret} - {error} with triggerMode: {triggerMode}", 0, $"{device0Name}.AcquireSamples");
            if (triggerMode == 3)
            {
                ret = aio.SetAiStartLevelEx(id, (short)requestedChannels[0], 1, 1);
                aio.GetErrorString(ret, out error);
                Logger.AioLog($"SetAiStartLevelEx: {ret} - {error}", 0, $"{device0Name}.AcquireSamples");        
            }
            if (device0SamplingClock == 0)
            {
                ret = aio.SetAiClockType(id, device0SamplingClock);
                aio.GetErrorString(ret, out error);
                //Console.WriteLine($"SetAiClockType: {ret} - {error}");
                Logger.AioLog($"SetAiClockType: {ret} - {error}", 0, $"{device0Name}.AcquireSamples");
                
                
                // Configure sampling settings
                ret = aio.SetAiSamplingClock(id, samplingClockPeriod); // example 1kHz
                aio.GetErrorString(ret, out error);
                //Console.WriteLine($"SetAiSamplingClock: {ret} - {error}");
                //Logger.AioLog($"SetAiSamplingClock: {ret} - {error}", 0, $"{device0Name}.AcquireSamples");
                if (ret != 0) Logger.AioLog("SetAiSamplingClock: " + error, 0, $"{device0Name}.AcquireSamples");
            }
            else if (device0SamplingClock == 1)
            {
                ret = aio.SetAiClockType(id, device0SamplingClock);
                aio.GetErrorString(ret, out error);
                //Console.WriteLine($"SetAiClockType: {ret} - {error}");
                Logger.AioLog($"SetAiClockType: {ret} - {error}", 0, $"{device0Name}.AcquireSamples");
                // External clock: set clock edge
                //----------------------------------------
                // Set the timing of external clock input
                ret = aio.SetAiClockEdge(id, externalClockEdge);
                aio.GetErrorString(ret, out error);
                Console.WriteLine($"SetAiClockEdge: {ret} - {error}");
                if (ret != 0) throw new Exception("SetAiClockEdge: " + error);
            }
            ret=aio.SetAiScanClock(id, conversionSpeed);
            aio.GetErrorString(ret, out error);
            //Console.WriteLine($"SetAiScanClock: {ret} - {error}");
            Logger.AioLog($"SetAiScanClock: {ret} - {error}", 0, $"{device0Name}.AcquireSamples");
            ret = aio.SetAiStopTimes(id, stopTimes);
            aio.GetErrorString(ret, out error);
            //Console.WriteLine($"SetAiStopTimes: {ret} - {error}");
            Logger.AioLog($"SetAiStopTimes: {ret} - {error}", 0, $"{device0Name}.AcquireSamples");
            
            aio.SetAiStopTrigger(id, 0);

            aio.ResetAiMemory(id);
            aio.ResetAiStatus(id);

            aio.StartAi(id);
            int stableCount = 0;
            // Wait for completion
            while (samplesperchannel < stopTimes)
            {
                ret=aio.GetAiStatus(id, out int status);
                int aiStatus = status;
                //Console.WriteLine($"[DEBUG] Status: 0x{status:X}");
                //Logger.AioLog($"[DEBUG] Status: 0x{status:X}", 0, $"{device0Name}.AcquireSamples");
                bool busy = (status & (int)CaioConst.AIS_BUSY) != 0;
                bool dataReady = (status & (int)CaioConst.AIS_DATA_NUM) != 0;
                bool errorFlag = (status & ((int)CaioConst.AIS_OFERR | (int)CaioConst.AIS_SCERR |
                                            (int)CaioConst.AIS_AIERR | (int)CaioConst.AIS_DRVERR)) != 0;
                //Console.WriteLine($"[DEBUG]status:{status} oferr: {(int)CaioConst.AIS_OFERR}, scerr: {(int)CaioConst.AIS_SCERR}, aierr: {(int)CaioConst.AIS_AIERR}, drverr: {(int)CaioConst.AIS_DRVERR}");
                //Logger.AioLog($"[DEBUG]status:{status} oferr: {(int)CaioConst.AIS_OFERR}, scerr: {(int)CaioConst.AIS_SCERR}, aierr: {(int)CaioConst.AIS_AIERR}, drverr: {(int)CaioConst.AIS_DRVERR}", 0, $"{device0Name}.AcquireSamples");
                if (errorFlag)
                {
                    aio.GetErrorString(ret, out error);
                    //Console.WriteLine("Error: " + error);
                    Logger.AioLog("Error: " + error, 1, $"{device0Name}.AcquireSamples");
                    if ((aiStatus & 0x00100000) != 0)
                    {
                        Logger.AioLog("Overflow: More conversion data than memory can store. " +
                            "If using FIFO, conversion stops. If using RING, old data is overwritten. " +
                            "If buffer overflows with software memory, conversion stops. " +
                            "Consider calling AioResetAiMemory, AioStartAi, or AioResetDevice.",1,$"{device0Name}.AcquireSamples");
                    }

                    if ((aiStatus & 0x00200000) != 0)
                    {
                        Logger.AioLog("Sampling clock period error: The sampling clock is too fast, or driver cannot keep up. " +
                            "Possible causes: impossible internal clock, slow driver processing, or external clock noise. " +
                            "Try increasing the clock period, reducing channel count, or checking for noise. " +
                            "Reset with AioResetAiStatus, AioStartAi, or AioResetDevice.",1, $"{device0Name}.AcquireSamples");
                    }

                    if ((aiStatus & 0x00400000) != 0)
                    {
                        Logger.AioLog("AD conversion error: Conversion did not complete. " +
                            "This usually indicates a hardware fault. Contact Contec support if this persists.",1, $"{device0Name}.AcquireSamples");
                    }

                    if ((aiStatus & 0x00800000) != 0)
                    {
                        Logger.AioLog("Driver spec error: Driver could not process in time. " +
                            "Often occurs with Sampling clock period error. " +
                            "Try reducing sampling speed or number of channels.",1, $"{device0Name}.AcquireSamples");
                    }

                    // Optionally, print the raw status for further debugging
                    //Console.WriteLine($"Raw AiStatus: 0x{aiStatus:X8}");
                    //Console.WriteLine("[WARN] Device error flag detected during acquisition.");
                    Logger.AioLog("[WARN] Device error flag detected during acquisition.", 1, $"{device0Name}.AcquireSamples");
                    break;
                }
                //if (!busy || dataReady) break;
                aio.GetAiSamplingCount(id, out samplesperchannel);
                if (samplesperchannel == 0)
                {
                    stableCount++;
                    if (stableCount > 10)
                    {
                        //Console.WriteLine("[INFO] No samples acquired — possibly no signal source connected.");
                        Logger.AioLog("[INFO] No samples acquired — possibly no signal source connected.", 0, $"{device0Name}.AcquireSamples");
                        break;
                    }
                }
                Thread.Sleep(50);
            }

            // Get sample count
            aio.GetAiSamplingCount(id, out int samplesPerChannel);
            if (samplesPerChannel <= 0)
            {
                //Console.WriteLine("[INFO] No valid samples. Returning zeroed buffer.");
                Logger.AioLog("[INFO] No valid samples. Returning zeroed buffer.", 1, $"{device0Name}.AcquireSamples");
                double[][] empty = new double[channels][];
                for (int ch = 0; ch < channels; ch++)
                    empty[ch] = new double[stopTimes]; // all zeros
                return empty;
            }
            long totalNeeded = (long)samplesPerChannel * channels;
            int floatsToAlloc = (int)Math.Min(totalNeeded, MAX_BUFFER_FLOATS);
            float[] flatData = new float[floatsToAlloc];
            int req = samplesPerChannel;
            ret = aio.GetAiSamplingDataEx(id, ref req, ref flatData);
            aio.GetErrorString(ret, out error);
            // Convert to double[][] efficiently
            if (ret != 0)
            {
                //Console.WriteLine($"[WARN] GetAiSamplingDataEx failed: {error}");
                Logger.AioLog($"[WARN] GetAiSamplingDataEx failed: {error}", 1, $"{device0Name}.AcquireSamples");
                double[][] fallback = new double[channels][];
                for (int ch = 0; ch < channels; ch++)
                    fallback[ch] = new double[stopTimes];
                return fallback;
            }
            int actualSamples = Math.Min(req, floatsToAlloc / channels);
            double[][] result = new double[channels][];
            for (int ch = 0; ch < channels; ch++)
            {
                double[] chData = new double[actualSamples];
                for (int s = 0; s < actualSamples; s++)
                    chData[s] = flatData[s * channels + ch];
                result[ch] = chData;
            }

            // Stop and restore 32-channel config
            aio.StopAi(id);
            aio.SetAiChannels(id, device0Channels);

            return result;
        }
        catch (Exception ex)
        {
            //Console.WriteLine($"[ERROR] AcquireSamples exception: {ex.Message}");
            Logger.AioLog($"[ERROR] AcquireSamples exception: {ex.Message}", 1, $"{device0Name}.AcquireSamples");
            double[][] fallback = new double[channels][];
            for (int ch = 0; ch < channels; ch++)
                fallback[ch] = new double[stopTimes];
            return fallback;
        }
        finally
        {
            try
            {
                aio.StopAi(id);
                aio.SetAiChannels(id, device0Channels);
            }
            catch { }
        }
    }

    // ==================== Acquire Samples (AIO001) ====================
    // ==================== Acquire Samples (AIO001) ====================
    private static double[][] AcquireSamplesAio001(short channels, int stopTimes)
    {
        const int MAX_BUFFER_FLOATS = 1024 * 1024;
        int ret;
        string error;
        int samplesperchannel = 0;

        try
        {
            
            ret = aio.SetAiChannels(aio001_id, channels);
            aio.GetErrorString(ret, out error);
            if (ret != 0) {
                Logger.AioLog($"[{device1Name}] SetAiChannels: " + error, 1, $"{device1Name}.AcquireSamplesAio001");
                throw new Exception($"[{device1Name}] SetAiChannels: " + error); }
            ret = aio.SetAiTransferMode(aio001_id, 0);
            aio.GetErrorString(ret, out error);
            //Console.WriteLine($"SetAiTransferMode: {ret} - {error}");
            Logger.AioLog($"SetAiTransferMode: {ret} - {error}", 0, $"{device1Name}.AcquireSamplesAio001");
            ret = aio.SetAiMemoryType(aio001_id, 0);
            aio.GetErrorString(ret, out error);
            //Console.WriteLine($"SetAiMemoryType: {ret} - {error}");
            Logger.AioLog($"SetAiMemoryType: {ret} - {error}", 0, $"{device1Name}.AcquireSamplesAio001");
            aio.SetAiStartTrigger(aio001_id, device1TriggerMode);
            if (device1TriggerMode == 3)
            {
                ret = aio.SetAiStartLevelEx(aio001_id, device1DataChannel, 1, 1);
                aio.GetErrorString(ret, out error);
                Logger.AioLog($"SetAiStartLevelEx: {ret} - {error}", 0, $"{device1Name}.AcquireSamplesAio001");
            }
            if (device1SamplingClock == 0)
            {
                ret = aio.SetAiClockType(aio001_id, device1SamplingClock);
                aio.GetErrorString(ret, out error);
                //Console.WriteLine($"SetAiClockType: {ret} - {error}");
                Logger.AioLog($"SetAiClockType: {ret} - {error}", 0, $"{device1Name}.AcquireSamplesAio001");


                // Configure sampling settings
                ret = aio.SetAiSamplingClock(aio001_id, device1SamplingClockPeriod); // example 1kHz
                aio.GetErrorString(ret, out error);
                //Console.WriteLine($"SetAiSamplingClock: {ret} - {error}");
                //Logger.AioLog($"SetAiSamplingClock: {ret} - {error}", 0, $"{device0Name}.AcquireSamples");
                if (ret != 0) Logger.AioLog("SetAiSamplingClock: " + error, 0, $"{device1Name}.AcquireSamplesAio001");
            }
            else if (device1SamplingClock == 1)
            {
                ret = aio.SetAiClockType(aio001_id, device1SamplingClock);
                aio.GetErrorString(ret, out error);
                //Console.WriteLine($"SetAiClockType: {ret} - {error}");
                Logger.AioLog($"SetAiClockType: {ret} - {error}", 0, $"{device1Name}.AcquireSamplesAio001");
                // External clock: set clock edge
                //----------------------------------------
                // Set the timing of external clock input
                ret = aio.SetAiClockEdge(aio001_id, 1);
                aio.GetErrorString(ret, out error);
                Console.WriteLine($"SetAiClockEdge: {ret} - {error}");
                if (ret != 0) throw new Exception("SetAiClockEdge: " + error);
            }
           
            ret=aio.SetAiScanClock(aio001_id, device1ConversionSpeed);
            aio.GetErrorString(ret, out error);
            //Console.WriteLine($"SetAiScanClock: {ret} - {error}");
            Logger.AioLog($"[{device1Name}] SetAiScanClock: {ret} - {error}", 0, $"{device1Name}.AcquireSamplesAio001");
            ret=aio.SetAiStopTimes(aio001_id, stopTimes);
            aio.GetErrorString(ret, out error);
            //Console.WriteLine($"SetAiStopTimes: {ret} - {error}");
            Logger.AioLog($"[{device1Name}] SetAiStopTimes: {ret} - {error}", 0, $"{device1Name}.AcquireSamplesAio001");
          
            aio.SetAiStopTrigger(aio001_id, 0);

            aio.ResetAiMemory(aio001_id);
            aio.ResetAiStatus(aio001_id);
            aio.StartAi(aio001_id);

            int stableCount = 0;
            while (samplesperchannel < stopTimes)
            {
                ret=aio.GetAiStatus(aio001_id, out int status);
                int aiStatus = status;
                //Console.WriteLine($"[DEBUG] Status: 0x{status:X}");
                //Logger.AioLog($"[DEBUG] Status: 0x{status:X}", 0, $"{device1Name}.AcquireSamplesAio001");
                bool busy = (status & (int)CaioConst.AIS_BUSY) != 0;
                bool dataReady = (status & (int)CaioConst.AIS_DATA_NUM) != 0;
                bool errorFlag = (status & ((int)CaioConst.AIS_OFERR | (int)CaioConst.AIS_SCERR |
                                            (int)CaioConst.AIS_AIERR | (int)CaioConst.AIS_DRVERR)) != 0;

                if (errorFlag)
                {
                    aio.GetErrorString(ret, out error);
                    //Console.WriteLine("Error: " + error);
                    Logger.AioLog("Error: " + error, 1, $"{device1Name}.AcquireSamplesAio001");
                    if ((aiStatus & 0x00100000) != 0)
                    {
                        Logger.AioLog("Overflow: More conversion data than memory can store. " +
                            "If using FIFO, conversion stops. If using RING, old data is overwritten. " +
                            "If buffer overflows with software memory, conversion stops. " +
                            "Consider calling AioResetAiMemory, AioStartAi, or AioResetDevice.", 1, $"{device1Name}.AcquireSamplesAio001");
                    }

                    if ((aiStatus & 0x00200000) != 0)
                    {
                        Logger.AioLog("Sampling clock period error: The sampling clock is too fast, or driver cannot keep up. " +
                            "Possible causes: impossible internal clock, slow driver processing, or external clock noise. " +
                            "Try increasing the clock period, reducing channel count, or checking for noise. " +
                            "Reset with AioResetAiStatus, AioStartAi, or AioResetDevice.", 1, $"{device1Name}.AcquireSamplesAio001");
                    }

                    if ((aiStatus & 0x00400000) != 0)
                    {
                        Logger.AioLog("AD conversion error: Conversion did not complete. " +
                            "This usually indicates a hardware fault. Contact Contec support if this persists.", 1, $"{device1Name}.AcquireSamplesAio001");
                    }

                    if ((aiStatus & 0x00800000) != 0)
                    {
                        Logger.AioLog("Driver spec error: Driver could not process in time. " +
                            "Often occurs with Sampling clock period error. " +
                            "Try reducing sampling speed or number of channels.", 1, $"{device1Name}.AcquireSamplesAio001");
                    }

                    // Optionally, print the raw status for further debugging
                    //Console.WriteLine($"Raw AiStatus: 0x{aiStatus:X8}");
                    //Console.WriteLine($"[{device1Name}][WARN] Device error flag detected during acquisition.");
                    Logger.AioLog($"[{device1Name}][WARN] Device error flag detected during acquisition.", 1, $"{device1Name}.AcquireSamplesAio001");
                    break;
                }

                aio.GetAiSamplingCount(aio001_id, out samplesperchannel);
                if (samplesperchannel == 0)
                {
                    stableCount++;
                    if (stableCount > 10)
                    {
                        //Console.WriteLine($"[{device1Name}][INFO] No samples acquired — possibly no signal source connected.");
                        Logger.AioLog($"[{device1Name}][INFO] No samples acquired — possibly no signal source connected.", 1, $"{device1Name}.AcquireSamplesAio001");
                        break;
                    }
        }

                Thread.Sleep(50);
    }

            aio.GetAiSamplingCount(aio001_id, out int samplesPerChannel);
            if (samplesPerChannel <= 0)
            {
                //Console.WriteLine($"[{device1Name}][INFO] No valid samples. Returning zeroed buffer.");
                Logger.AioLog($"[{device1Name}][INFO] No valid samples. Returning zeroed buffer.", 1, $"{device1Name}.AcquireSamplesAio001");
                double[][] empty = new double[channels][];
                for (int ch = 0; ch < channels; ch++)
                    empty[ch] = new double[stopTimes];
                return empty;
            }

            long totalNeeded = (long)samplesPerChannel * channels;
            int floatsToAlloc = (int)Math.Min(totalNeeded, MAX_BUFFER_FLOATS);
            float[] flatData = new float[floatsToAlloc];
            int req = samplesPerChannel;

            ret = aio.GetAiSamplingDataEx(aio001_id, ref req, ref flatData);
            aio.GetErrorString(ret, out error);
            if (ret != 0)
            {
                //Console.WriteLine($"[{device1Name}][WARN] GetAiSamplingDataEx failed: {error}");
                Logger.AioLog($"[{device1Name}][WARN] GetAiSamplingDataEx failed: {error}", 1, $"{device1Name}.AcquireSamplesAio001");
                double[][] fallback = new double[channels][];
                for (int ch = 0; ch < channels; ch++)
                    fallback[ch] = new double[stopTimes];
                return fallback;
            }

            int actualSamples = Math.Min(req, floatsToAlloc / channels);
            double[][] result = new double[channels][];
            for (int ch = 0; ch < channels; ch++)
            {
                double[] chData = new double[actualSamples];
                for (int s = 0; s < actualSamples; s++)
                    chData[s] = flatData[s * channels + ch];
                result[ch] = chData;
            }

            aio.StopAi(aio001_id);
            aio.SetAiChannels(aio001_id, device1Channels);

            return result;
        }
        catch (Exception ex)
        {
            //Console.WriteLine($"[ERROR][{device1Name}] AcquireSamples exception: {ex.Message}");
            Logger.AioLog($"[ERROR][{device1Name}] AcquireSamples exception: {ex.Message}", 1, $"{device1Name}.AcquireSamplesAio001");
            double[][] fallback = new double[channels][];
            for (int ch = 0; ch < channels; ch++)
                fallback[ch] = new double[stopTimes];
            return fallback;
        }
        finally
        {
            try
            {
                aio.StopAi(aio001_id);
                aio.SetAiChannels(aio001_id, device1Channels);
            }
            catch { }
        }
    }

    private static void LoadDeviceNamesFromConfig()
    {
        string configPath = Path.Combine(
     Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\")),
     "config",
     "Config.ini"
 );

        if (!File.Exists(configPath))
        {
            
            Logger.AioLog($"[WARN] Config.ini not found at {configPath}. Using default device names.", 1, "LoadDeviceNamesFromConfig");
            return;
        }

        try
        {
            string currentSection = "";
            foreach (var line in File.ReadLines(configPath))
            {
                var trimmed = line.Trim();
                if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith(";") || trimmed.StartsWith("#"))
                    continue;

                if (trimmed.StartsWith("[") && trimmed.EndsWith("]"))
                {
                    currentSection = trimmed[1..^1];
                    continue;
                }

                var eqIndex = trimmed.IndexOf('=');
                if (eqIndex <= 0) continue;

                var key = trimmed[..eqIndex].Trim();
                var value = trimmed[(eqIndex + 1)..].Trim();

                if (currentSection == "CONTEC1")
                {
                    if (key == "DeviceName")
                        device0Name = value;
                    else if (key == "Channels" && short.TryParse(value, out short ch0))
                        device0Channels = ch0;
                    else if (key == "Range" && short.TryParse(value, out short range))
                        device0Range = range;
                    else if (key == "SamplingClockPeriod" && float.TryParse(value, out float period))
                        device0SamplingClockPeriod = period;
                    else if (key == "SamplingClock" && short.TryParse(value, out short clock))
                        device0SamplingClock = clock;
                    else if (key == "ConversionSpeed" && float.TryParse(value, out float convSpeed))
                        device0ConversionSpeed = convSpeed;
                    else if (key == "TriggerMode" && short.TryParse(value, out short triggerMode))
                        device0TriggerMode = triggerMode;
                    else if (key == "TrgConfig")
                        trgConfigChannels = ParseIntArray(value);
                    else if (key == "VVTDZChannels")
                        VVTDZChannels = ParseIntArray(value);

                }
                else if (currentSection == "CONTEC2")
                {
                    if (key == "DeviceName")
                        device1Name = value;
                    else if (key == "Channels" && short.TryParse(value, out short ch1))
                        device1Channels = ch1;
                    else if(key== "DataChannel" && short.TryParse(value, out short dataCh))
                        device1DataChannel = dataCh;
                    else if (key == "Range" && short.TryParse(value, out short range))
                        device1Range = range;
                    else if (key == "SamplingClockPeriod" && float.TryParse(value, out float period))
                        device1SamplingClockPeriod = period;
                    else if (key == "SamplingClock" && short.TryParse(value, out short clock))
                        device1SamplingClock = clock;
                    else if (key == "ConversionSpeed" && float.TryParse(value, out float convSpeed))
                        device1ConversionSpeed = convSpeed;
                    else if (key == "TriggerMode" && short.TryParse(value, out short triggerMode))
                        device1TriggerMode = triggerMode;
                }

            }
           


            //Logger.AioLog($"[CONFIG] Loaded devices: {device0Name}, {device1Name}", 1, "LoadDeviceNamesFromConfig");
}
        catch (Exception ex)
        {
            
            Logger.AioLog($"[ERROR] Failed to read Config.ini: {ex.Message}. Using defaults.", 1, "LoadDeviceNamesFromConfig");
        }
    }
    private static int[] ParseIntArray(string value)
    {
        // Remove brackets and whitespace: "[1,2,3,4,7]" -> "1,2,3,4,7"
        var cleaned = value.Trim().TrimStart('[').TrimEnd(']');
        if (string.IsNullOrEmpty(cleaned))
            return [];

        return cleaned.Split(',', StringSplitOptions.RemoveEmptyEntries)
                      .Select(s => int.TryParse(s.Trim(), out int n) ? n : -1)
                      .Where(n => n >= 0)
                      .ToArray();
    }

}
