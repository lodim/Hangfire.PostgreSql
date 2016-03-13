using System;
using System.Collections.Generic;
using System.Configuration;
using Example.WebApp.Properties;

namespace Example.WebApp
{
    public static class ConfigurationService
    {
        private static readonly Dictionary<string, object> ConfigurationSettings = new Dictionary<string, object>();
        private static readonly object Lock = new object();

        public static string DefaultConnectionString => ConfigurationService.GetConnectionString("DefaultConnection");

        public static string GetConnectionString(string connectionStringName)
        {
            if (string.IsNullOrEmpty(connectionStringName))
                throw new ArgumentNullException(nameof(connectionStringName));

            object connectionStringValue;

            lock (ConfigurationService.Lock)
            {
                if (ConfigurationService.ConfigurationSettings.TryGetValue(connectionStringName, out connectionStringValue) == false)
                {
                    connectionStringValue = ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString;
                    if (ConfigurationService.ConfigurationSettings.ContainsKey(connectionStringName) == false)
                        ConfigurationService.ConfigurationSettings.Add(connectionStringName, connectionStringValue);
                }
            }

            return connectionStringValue.ToString();
        }

        public static T GetAppSetting<T>(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            object settingValue;

            lock (ConfigurationService.Lock)
            {
                if (ConfigurationService.ConfigurationSettings.TryGetValue(key, out settingValue) == false)
                {
                    settingValue = ConfigurationManager.AppSettings[key];
                    if (ConfigurationService.ConfigurationSettings.ContainsKey(key) == false)
                        ConfigurationService.ConfigurationSettings.Add(key, settingValue);
                }
            }

            if (settingValue is T)
                return (T)settingValue;

            try
            {
                return (T)Convert.ChangeType(settingValue, typeof(T));
            }
            catch (InvalidCastException)
            {
                return default(T);
            }
        }

        public static T GetApplicationSetting<T>(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            object settingValue;

            lock (ConfigurationService.Lock)
            {
                if (ConfigurationService.ConfigurationSettings.TryGetValue(key, out settingValue) == false)
                {
                    settingValue = Settings.Default[key];
                    if (ConfigurationService.ConfigurationSettings.ContainsKey(key) == false)
                        ConfigurationService.ConfigurationSettings.Add(key, settingValue);
                }
            }

            if (settingValue is T)
                return (T)settingValue;

            try
            {
                return (T)Convert.ChangeType(settingValue, typeof(T));
            }
            catch (InvalidCastException)
            {
                return default(T);
            }
        }
    }
}