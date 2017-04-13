﻿using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace LargeData.Client
{
    public class RestClient
    {
        /// <summary>
        /// [POST] execute a rest uri
        /// </summary>
        /// <typeparam name="T">request object type</typeparam>
        /// <typeparam name="V">response object type</typeparam>
        /// <param name="requestObject">request object</param>
        /// <param name="requestUri">request uri</param>
        /// <param name="baseUrl">base url</param>
        /// <returns></returns>
        public static async Task<V> Execute<T, V>(T requestObject, string requestUri, string baseUrl)
        {
            V responseObj = default(V);
            using (HttpClient client = new HttpClient())
            {
                client.BaseAddress = new Uri(baseUrl);
                var response = await client.PostAsync(requestUri, new StringContent(JsonConvert.SerializeObject(requestObject), Encoding.UTF8, "application/json"));
                if (!response.IsSuccessStatusCode)
                {
                    throw new HttpRequestException("Server failes to respond");
                }

                var responseStream = await response.Content.ReadAsStreamAsync();
                using (StreamReader streamReader = new StreamReader(responseStream))
                {
                    using (JsonReader jsonReader = new JsonTextReader(streamReader))
                    {
                        JsonSerializer serializer = new JsonSerializer();
                        responseObj = serializer.Deserialize<V>(jsonReader);
                    }
                }
            }
            return responseObj;
        }

        public static async Task<Byte[]> ExecuteForByteArray<T>(T requestObject, string requestUri, string baseUrl)
        {
            Byte[] responseObj = null;
            using (HttpClient client = new HttpClient())
            {
                client.BaseAddress = new Uri(baseUrl);
                var response = await client.PostAsync(requestUri, new StringContent(JsonConvert.SerializeObject(requestObject), Encoding.UTF8, "application/json"));
                if (!response.IsSuccessStatusCode)
                {
                    throw new HttpRequestException("Server failes to respond");
                }
                var responseBytes = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
                responseObj = responseBytes;
            }
            return responseObj;
        }
    }
}
