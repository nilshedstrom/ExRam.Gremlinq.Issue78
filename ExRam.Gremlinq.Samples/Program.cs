using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using ExRam.Gremlinq.Core;
using ExRam.Gremlinq.Providers.WebSocket;
using Microsoft.Extensions.Logging;

// Put this into static scope to access the default GremlinQuerySource as "g". 
using static ExRam.Gremlinq.Core.GremlinQuerySource;

namespace ExRam.Gremlinq.Samples
{
    public class Program
    {
        private readonly IGremlinQuerySource _g;

        public Program()
        {
            var endPoint = "localhost";
            var port = 8901;
            var databaseId = "database";
            var containerId = "container1";
            var authKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

            var logger = LoggerFactory
                .Create(builder => builder
                    .AddFilter(__ => true)
                    .AddConsole())
                .CreateLogger("Queries");

            _g = g
                .ConfigureEnvironment(env => env
                    .UseLogger(logger)
                    //Since the Vertex and Edge classes contained in this sample implement IVertex resp. IEdge,
                    //setting a model is actually not required as long as these classes are discoverable (i.e. they reside
                    //in a currently loaded assembly). We explicitly set a model here anyway.
                    .UseModel(GraphModel
                        .FromBaseTypes<Vertex, Edge>(lookup => lookup
                            .IncludeAssembliesOfBaseTypes())
                        //For CosmosDB, we exclude the 'PartitionKey' property from being included in updates.
                        .ConfigureProperties(model => model
                            .ConfigureElement<Vertex>(conf => conf
                                .IgnoreOnUpdate(x => x.PartitionKey))))
                    .UseCosmosDb(builder => builder
                        .At(new Uri($"ws://{endPoint}:{port}/"), databaseId, containerId)
                        //.At(uri, databaseName, graphName)
                        .AuthenticateBy(authKey)
                        .ConfigureWebSocket(builder => builder
                            //Disable query logging for a noise free console output.
                            //Enable logging by setting the verbosity to anything but None.
                            .ConfigureQueryLoggingOptions(o => o
                                .SetQueryLoggingVerbosity(QueryLoggingVerbosity.QueryAndParameters)))));
        }

        public async Task Run()
        {
            await Create_the_graph();
            await My_Queries();
            Console.Write("Press any key...");
            Console.Read();
        }

        private async Task Create_the_graph()
        {
            // Create a graph very similar to the one
            // found at http://tinkerpop.apache.org/docs/current/reference/#graph-computing.

            // Uncomment to delete the whole graph on every run.
            await _g.V().Drop();

            var josh = await _g
                .AddV(new Person { Name = "Josh", Age = 32 })
                .FirstAsync();

            var charlie = await _g
                .AddV(new Dog { Name = "Charlie", Age = 2 })
                .FirstAsync();

            await _g
                .V(josh.Id)
                .AddE<Owns>()
                .To(__ => __
                    .V(charlie.Id))
                .FirstAsync();
        }

        private static async Task Main()
        {
            await new Program().Run();
        }

        private async Task My_Queries()
        {
            //Check if the pet Charlie has owner which is not more than 30 years older
            //This query will always return nothing because sum is a reducing barrier step http://kelvinlawrence.net/book/PracticalGremlin.html#rbarriers
            //and it will cause the database to forget the values stored in ownerAge and pet.
            var pet = await _g.V<Pet>().Where(p => p.Name.Value == "Charlie").Limit(1)
                .As((__, pet) => __
                    .Where(__ => __
                        .In<Owns>().OfType<Person>()
                        .Values(x => x.Age)
                        .As((__, ownerAge) => __
                            .Select(pet)
                            .Where(pet => pet.Age < ownerAge.Value)
                            .Local(__ => __.Values(x => x.Age).Union(__ => __.Identity(), __ => __.Constant(30)).Sum())
                            .As((__, ownerMaxAge) => __
                                .Where(_ => ownerAge.Value <= ownerMaxAge.Value)))));

            //Very nice to have syntax but currently not supported by Gremlinq. Of course I am accepting pull requests...
            //var pet2 = await _g.V<Pet>().Where(p => p.Name.Value == "Charlie").Limit(1)
            //    .As((__, pet) => __
            //        .Where(__ => __
            //            .In<Owns>().OfType<Person>()
            //            .Where(owner => pet.Value.Age < owner.Age)
            //            .Where(owner => owner.Age <= pet.Value.Age + 30)));
        }

        private static Expression<Func<Result, bool>> GetFilterExpression()
        {
            return result => result.ownerAge > result.petAge && result.ownerAge <= result.ownerMaxAge;
        }

        internal class Result
        {
            public long ownerAge;
            public long petAge;
            public long ownerMaxAge;
        }
    }

}
