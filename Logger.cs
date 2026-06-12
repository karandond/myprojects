using System;
using System.IO;

namespace AioCardService
{
    public class Logger
    {
        private static readonly object _aioLogLock = new();
        private static readonly string logDirectory = Path.Combine(
            Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\")),
            "log files");

        private static readonly string logFilePath = Path.Combine(
            logDirectory, $"log_{DateTime.Now:yyyyMMdd}.txt");

        private static readonly string plcReadlogFilePath = Path.Combine(
           logDirectory, $"plcReadlog_{DateTime.Now:yyyyMMdd}.txt");
        private static readonly string aioLogFilePath = Path.Combine(
           logDirectory, $"aioLog_{DateTime.Now:yyyyMMdd}.txt");
        // Configurable retention period (default: 2 days)
        // Log activation flags - cached for performance
        private static readonly bool isLogEnabled;
        private static readonly bool isPlcReadLogEnabled;
        private static readonly bool isAioLogEnabled;
        private static int logRetentionDays = 2;
        /// <summary>
        /// Sets the log retention period in days.
        /// </summary>
        public static void SetRetentionDays(int days)
        {
            if (days > 0)
                logRetentionDays = days;
        }
        /// <summary>
        /// Deletes log files older than the configured retention period.
        /// </summary>
        public static void CleanupOldLogs()
        {
            try
            {
                if (!Directory.Exists(logDirectory))
                    return;

                 var cutoffDate = DateTime.Now.AddDays(-logRetentionDays);
                var logFiles = Directory.GetFiles(logDirectory, "*.txt");

                foreach (var file in logFiles)
                {
                    var fileInfo = new FileInfo(file);
                    if (fileInfo.CreationTime < cutoffDate)
                    {
                        try
                        {
                            fileInfo.Delete();
                            
                        }
                        catch (Exception ex)
                        {
                            Logger.AioLog($"Failed to delete log file {fileInfo.Name}: {ex.Message}", 1);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.AioLog($"Log cleanup failed: {ex.Message}", 1);
            }
        }
        static Logger()
        {
            LoadLogActivationSettings(out isLogEnabled, out isPlcReadLogEnabled, out isAioLogEnabled);
            try
            {
                if ((isLogEnabled || isPlcReadLogEnabled || isAioLogEnabled) && !Directory.Exists(logDirectory))
                    Directory.CreateDirectory(logDirectory);
                CleanupOldLogs();
            }
            catch (Exception ex)
            {
                Logger.AioLog($"Logger initialization failed: {ex.Message}", 1);
            }
        }
        private static void LoadLogActivationSettings(out bool logEnabled, out bool plcReadLogEnabled, out bool aioLogEnabled)
        {
            // Default to disabled for performance
            logEnabled = false;
            plcReadLogEnabled = false;
            aioLogEnabled = false;

            string configPath = Path.Combine(
                Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\")),
                "config",
                "Config.ini");

            if (!File.Exists(configPath))
            {
                Console.WriteLine($"[WARN] Config.ini not found at {configPath}. Logging disabled.");
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
                        // Early exit once we've passed LogActivation section
                        if (currentSection != "LogActivation" && logEnabled == false && plcReadLogEnabled == false && aioLogEnabled == false)
                            continue;
                        if (currentSection != "LogActivation" && (logEnabled || plcReadLogEnabled || aioLogEnabled))
                            break; // We've read what we need
                        continue;
                    }

                    if (currentSection == "LogActivation")
                    {
                        var eqIndex = trimmed.IndexOf('=');
                        if (eqIndex <= 0) continue;

                        var key = trimmed[..eqIndex].Trim();
                        var value = trimmed[(eqIndex + 1)..].Trim();

                        switch (key)
                        {
                            case "log":
                                logEnabled = value == "1";
                                break;
                            case "plcReadlog":
                                plcReadLogEnabled = value == "1";
                                break;
                            case "aioLog":
                                aioLogEnabled = value == "1";
                                break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed to read log activation settings: {ex.Message}");
            }
        }
      


        public static void AioLog(string message, short type, string relation = "General")
        {
            if (!isAioLogEnabled) return;
            string typeString = type switch
            {
                0 => "INFO",
                1 => "ERROR",
                2 => "WARNING",
                _ => "UNKNOWN"
            };

            string logMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} | {typeString} | [{relation}]: {message}";

            Console.WriteLine(logMessage);

            try
            {
                lock (_aioLogLock)
                {
                File.AppendAllText(aioLogFilePath, logMessage + Environment.NewLine);
            }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error writing log to file: {ex.Message}");
            }
        }
    }
}
