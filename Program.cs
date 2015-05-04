using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;

namespace DocumentDBSample
{
    class Program
    {
        private static string EndPointUrl;
        private static string AuthorizationKey;

        private static DocumentClient DocumentDBClient;

        internal sealed class Car
        {
            public string Make { get; set; }
            public string Model { get; set; }
            public string Color { get; set; }
        }

        internal sealed class FamousPerson
        {
            public string Name { get; set; }
            public string Address { get; set; }
            public Car[] Cars { get; set; }
        }

        static void Main(string[] args)
        {
            try
            {
                EndPointUrl = ConfigurationManager.AppSettings["EndPointUrl"];
                AuthorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];
                DoTheWork().Wait();
            }
            catch (DocumentClientException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("All done!");
            }         
        }

        private static async Task DoTheWork()
        {
            string databaseName = "TestDB";
            string collectionName = "TestCollection1";

            bool createCollection = true;

           using (DocumentDBClient = new DocumentClient(new Uri(EndPointUrl), AuthorizationKey))
            {
                DocumentCollection documentCollection = null; ; 

                // Create the collection
                if (createCollection)
                {
                    documentCollection = await CreateCollection(databaseName, collectionName);
                }

                List<FamousPerson> famousPeople = new List<FamousPerson>();

                for (int i = 0; i <= 5; i++)
                {
                    FamousPerson famousPerson = new FamousPerson();
                    famousPerson.Name = String.Format("FamousPerson{0}", i);
                    famousPerson.Address = String.Format("FamousPerson{0}'s Address", i);
                    famousPerson.Cars = new Car[] {
                        new Car{Make = "Make1", Model = "Model1", Color = "Color1"},
                        new Car{Make = "Make2", Model = "Model2", Color = "Color2"},
                        new Car{Make = "Make3", Model = "Model3", Color = "Color3"},
                    };
                    famousPeople.Add(famousPerson);
                }

                // Insert new documents into the collection
                if (documentCollection != null)
                    await InsertDocuments(documentCollection, famousPeople);
            }
        }

        private static async Task<DocumentCollection> CreateCollection(string databaseName, string collectionName)
        {

            // First get the database
            Database database = DocumentDBClient.CreateDatabaseQuery()
                .Where(db => db.Id == databaseName)
                .AsEnumerable().FirstOrDefault();

            if (database == null)
            {
                Exception e = new Exception(String.Format("Database {0} does not exist. Please try another database", databaseName));
            }

            // Now check to see if the collection exists
            DocumentCollection documentCollection = DocumentDBClient.CreateDocumentCollectionQuery(
                database.CollectionsLink)
                .Where(c => c.Id == collectionName)
                .AsEnumerable().FirstOrDefault();

            // Create the collection if it doesn't exist
            if (documentCollection == null)
            {
                // Now create the collection using the lowest performance tier available (currently, S1)
                documentCollection = await DocumentDBClient.CreateDocumentCollectionAsync(
                    database.CollectionsLink, 
                    new DocumentCollection { Id = collectionName }, 
                    new RequestOptions { OfferType = "S1" });
            }

            return documentCollection;
        }

        private static async Task InsertDocuments(DocumentCollection documentCollection, List<FamousPerson> famousPeople)
        {
            foreach (FamousPerson fp in famousPeople)
            {
                var doc = await DocumentDBClient.CreateDocumentAsync(documentCollection.DocumentsLink, fp);
            }
        }
    }
}
