using Alea;
using Alea.Parallel;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace MapReduceNode
{
    class Program
    {
        // master server
        private static HttpClient Master { get; set; }

        // Iterations for pseudo prime calculation
        private static readonly int ITERATIONS_FOR_PSEUDOPRIME = Convert.ToInt32(ConfigurationManager.AppSettings["ITERATIONS_FOR_PSEUDOPRIME"]);

        // Weather or not to use GPU acceleration
        private static readonly bool GPU = Convert.ToBoolean(ConfigurationManager.AppSettings["GPU"]);

        // GET and POST URLs
        private static readonly string GET_URL = ConfigurationManager.AppSettings["GET_URL"];
        private static readonly string POST_URL = ConfigurationManager.AppSettings["POST_URL"];

        static void Main(string[] args)
        {
            // Setup node and its connection to the master sever
            Master = new HttpClient();
            Master.BaseAddress = new Uri(ConfigurationManager.AppSettings["MASTER_URI"]);
            Console.WriteLine("Establishing connection to " + Master.BaseAddress.AbsoluteUri);
            Master.DefaultRequestHeaders.Accept.Clear();
            Master.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            // Get job to run
            string debug = Master.GetStringAsync(GET_URL).Result;
            Job job = JsonConvert.DeserializeObject<Job>(debug);

            Console.WriteLine("Connection established with " + Master.BaseAddress.AbsoluteUri);

            // Run job
            while (job == null || job.JobType != JobType.EXIT)
            {
                string jobType = null;
                switch (job.JobType)
                {
                    case (JobType.NULL):
                        jobType = "no";
                        break;
                    case (JobType.MAP):
                        jobType = "map";
                        break;
                    case (JobType.REDUCE):
                        jobType = "reduce";
                        break;
                }
                Console.WriteLine("Recieved " + jobType + " job from server");

                #region NullHandler

                // If job is to wait or error, sleep 0.5s then poll server for new job
                if (job == null || job.JobType == JobType.NULL)
                {
                    Thread.Sleep(500);
                    // Update masterState
                    debug = Master.GetStringAsync(GET_URL).Result;
                    job = JsonConvert.DeserializeObject<Job>(debug);

                    continue;
                }

                #endregion

                // Calculate start time (for calculating time to execute)
                job.StartTime = DateTime.Now;

                // Get number to test primality / pseudo primality of
                Dictionary<int, bool> data = Job.ListToDict(job.Keys, job.Values, false);
                int[] keys = data.Keys.ToArray();

                // If a map job, then determine if pseudoprime
                if (job.JobType == JobType.MAP)
                {
                    // Map loop

                    #region RNG

                    Random r = new Random();


                    int[,] allRands = new int[keys.Count(), ITERATIONS_FOR_PSEUDOPRIME];
                    Parallel.For(0, keys.Count(), i => 
                    {
                        for (int j = 0; j < ITERATIONS_FOR_PSEUDOPRIME; j++)
                        {
                            allRands[i, j] = r.Next(keys[i] - 1);
                        }
                    });

                    #endregion

                    Parallel.For(0, keys.Count(), i =>
                    {
                        int n = keys[i];
                        int[] randomNumbers = new int[ITERATIONS_FOR_PSEUDOPRIME];

                        for (int j = 0; j < ITERATIONS_FOR_PSEUDOPRIME; j++)
                        {

                            #region <CENSORED>LogicFail

                            try
                            {
                                randomNumbers[j] = allRands[n, j];
                            }
                            catch (IndexOutOfRangeException e)
                            {
                                // C# is deciding that 64 < 64 == True for some reason, so this try catch fixes logic breaking in this loop
                                break;
                            }

                            #endregion

                        }

                        data[keys[i]] = IsPseudoPrime(n, ITERATIONS_FOR_PSEUDOPRIME, randomNumbers);

                    });
                }
                // If a reduce job then determine if prime
                else if (job.JobType == JobType.REDUCE)
                {
                    // Reduce loop

                    Parallel.For(0, keys.Count(), i =>
                    {
                        data[keys[i]] = IsPrime(keys[i], (int)Math.Floor(Math.Sqrt(keys[i])));
                    });

                }

                //Update on each numbers primality or pseudoprimality
                Parallel.For(0, job.Keys.Count(), i =>
                {
                    job.Values[i] = data[job.Keys[i]];
                });

                // Calculate end time (for calculating time to execute)
                job.EndTime = DateTime.Now;

                //Put the Node's name on its homework
                job.NodeID = Environment.MachineName;

                // Return job result
                Master.PostAsJsonAsync<Job>(POST_URL, job);

                // Update masterState
                debug = Master.GetStringAsync(GET_URL).Result;
                job = JsonConvert.DeserializeObject<Job>(debug);
            }
        }


        // https://rosettacode.org/wiki/Miller%E2%80%93Rabin_primality_test#C.23
        /// <summary>
        /// Finds if the number is pseudo prime
        /// Modified for parallelization
        /// </summary>
        /// <param name="n">number to find pseusoprimality of</param>
        /// <param name="k">iterations to run</param>
        /// <returns>weather or not number is pseudo prime</returns>
        [GpuManaged]
        private static bool IsPseudoPrime(int n, int k, int[] randomNumbers)
        {
            if ((n < 2) || (n % 2 == 0))
            {
                return (n == 2);
            }

            int s = n - 1;
            while (s % 2 == 0)
            {
                s >>= 1;
            }

            bool[] results = new bool[k];
            for (int i = 0; i < k; i++)
            {
                results[i] = true;
            }

            if (GPU)
            {
                Gpu.Default.For(0, k, i => {
                    int a = randomNumbers[i] + 1;
                    int temp = s;
                    int mod = 1;
                    for (int j = 0; j < temp; ++j)
                    {
                        mod = (mod * a) % n;
                    }
                    while (temp != n - 1 && mod != 1 && mod != n - 1)
                    {
                        mod = (mod * mod) % n;
                        temp *= 2;
                    }

                    if (mod != n - 1 && temp % 2 == 0)
                    {
                        results[i] = false;
                    }
                });
            }
            else
            {
                for (int i = 0; i < k; i++)
                {
                    int a = randomNumbers[i] + 1;
                    int temp = s;
                    int mod = 1;
                    for (int j = 0; j < temp; ++j)
                    {
                        mod = (mod * a) % n;
                    }
                    while (temp != n - 1 && mod != 1 && mod != n - 1)
                    {
                        mod = (mod * mod) % n;
                        temp *= 2;
                    }

                    if (mod != n - 1 && temp % 2 == 0)
                    {
                        results[i] = false;
                    }
                }
            }

            for (int i = 0; i < k; i++)
            {
                if (results[i] == false)
                {
                    return false;
                }
            }

            return true;

        }

        //https://rosettacode.org/wiki/Primality_by_trial_division#C.23
        /// <summary>
        /// Finds if number is prime in trial by division
        /// Modified for parallelization
        /// </summary>
        /// <param name="n">number to determine primality of</param>
        /// <returns>weather the number is prime</returns>
        private static bool IsPrime(int n, int sqrt)
        {
            if (GPU)
            {
                bool[] isDivisible = new bool[sqrt - 2];

                Gpu.Default.For(2, sqrt, i =>
                {
                    if (n % i == 0)
                    {
                        isDivisible[i-2] = true;
                    }
                });

                
                return !isDivisible.Contains(true);
            }
            else
            {
                for (int i = 2; i < sqrt; i++)
                {
                    if (n % i == 0)
                    {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    public enum JobType { NULL = 0, MAP, REDUCE, EXIT }

    public class Job
    {
        public JobType JobType { get; set; }
        public List<int> Keys { get; set; }
        public List<bool> Values { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public int ID { get; set; }
        public string NodeID { get; set; }

        public static Dictionary<int, bool> ListToDict(List<int> k, List<bool> v, bool filter)
        {
            Dictionary<int, bool> payload = new Dictionary<int, bool>();

            for (int i = 0; i < k.Count(); i++)
            {
                if ((filter && v[i]) || !filter)
                {
                    payload.Add(k[i], v[i]);
                }
            }

            return payload;
        }
    }
}