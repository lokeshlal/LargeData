namespace LargeData
{
    public interface ICache
    {
        T Get<T>(string key);
        void Put<T>(string key, T value);
        void Remove(string key);
    }
}
