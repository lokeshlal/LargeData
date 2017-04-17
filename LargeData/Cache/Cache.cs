using System;
using System.Runtime.Caching;
using System.Web;
using System.Web.SessionState;

namespace LargeData
{
    /// <summary>
    /// server side cache element, can be replaced by the any other cache implementation (we have used SQL server persistance)
    /// </summary>
    public class Cache : ICache
    {
        public T Get<T>(string key)
        {
            return (T)MemoryCache.Default.Get(key);
        }

        public void Put<T>(string key, T value)
        {
            var cacheItemPolicy = new CacheItemPolicy();
            cacheItemPolicy.AbsoluteExpiration = DateTimeOffset.Now.AddDays(1);

            if (MemoryCache.Default[key] != null)
            {
                MemoryCache.Default[key] = value;
            }
            else
            {
                MemoryCache.Default.Add(key, value, cacheItemPolicy);
            }
        }

        public void Remove(string key)
        {
            MemoryCache.Default.Remove(key);
        }
    }
}
