using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Dapper;
using Nest;
using PowerArgs;
using ServiceStack.Text;

namespace ConsoleApplication1
{
    class Program
    {
        private const string _index = "item";

        static void Main(string[] args)
        {
            try
            {
                var parsed = Args.Parse<MyArgs>(args);
                if (parsed.Help)
                {
                    ArgUsage.GetStyledUsage<MyArgs>().Write();
                }
                else
                {
                    HttpResponseMessage response = null;
                    switch (parsed.Action)
                    {
                        case Action.CreateIndex:
                            CreateIndex(parsed.Server, parsed.Index);
                            Console.WriteLine("Index created");
                            break;
                        case Action.Index:
                            Index(parsed.Server, parsed.Index);
                            break;
                        case Action.LargeIndex:
                            DeleteIndex(parsed.Server, parsed.Index);
                            CreateLargeIndex(parsed.Server, parsed.Index);
                            IndexLarge(parsed.Server, parsed.Index);
                            break;
                        case Action.Query:
                            Query(parsed.Server, parsed.Index);
                            break;
                        case Action.DeleteIndex:
                            DeleteIndex(parsed.Server, parsed.Index);
                            break;
                        default:
                            ArgUsage.GetStyledUsage<MyArgs>().Write();
                            break;
                    }

                    response.PrintDump();
                }
            }
            catch (ArgException ex)
            {
                Console.WriteLine(ex.Message);
                ArgUsage.GetStyledUsage<MyArgs>().Write();
            }

            Console.ReadKey();
        }

        private static ElasticClient CreateClient(string server, string index)
        {
            var elasticSettings = new ConnectionSettings(new Uri(server))
                                        .SetDefaultIndex(index)
                                        .SetMaximumAsyncConnections(20);
            var client = new ElasticClient(elasticSettings);
            return client;
        }

        private static async void Query(string server, string index)
        {
            var client = CreateClient(server, index);
            var results = client.Search<Item>(s => s
        .From(0)
        .Size(10)
        .Fields(f => f.Title, f => f.Body)
        .SortAscending(f => f.ItemId)
        .SortDescending(f => f.Title)
        .Query(q => q.Term(f => f.Title, "klocka", Boost: 2.0))

);
            results.PrintDump();
        }

        private static void Index(string server, string index)
        {
            var client = CreateClient(server, index);
            foreach (var item in CreateItems())
            {
                client.Index(item);
            }

        }

        private static string sql = @"
SELECT TOP {0} P.[ProductID]
      ,[ManufacturerPartnerID]
      ,[PartNumber]
      ,[MainDescription]
      ,De.Weight
      ,D.Description
  FROM [tProduct] AS P
       JOIN tProductPartnerX AS PPX ON (PPX.ProductID = P.ProductID)
       JOIN tDescription AS D ON (D.ProductPartnerID = PPX.ProductPartnerID)
       JOIN tDetail AS De ON (De.ProductPartnerID = PPx.ProductPartnerID)";
        private static void IndexLarge(string server, string index)
        {
            const int top = 200000;
            const int batchCount = 20;
            const int batchSize = top / batchCount;
            var client = CreateClient(server, index);
            var responses = new List<Task<IBulkResponse>>();
            Console.WriteLine("Start indexing");
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DB"].ConnectionString))
            {
                conn.Open();
                var list = conn.Query<FSProduct>(string.Format(sql, top));
                var taskList = Enumerable.Range(0, batchCount).Select(x =>
                                                                      {
                                                                          Console.WriteLine("Loop {0}", x);
                                                                          return client.IndexManyAsync(list.Skip(x * batchSize).Take(batchSize));
                                                                      });
                Task.WhenAll(taskList);

                stopwatch.Stop();
                Console.WriteLine("Time taken {0}:{1}:{2}", stopwatch.Elapsed.Minutes, stopwatch.Elapsed.Seconds, stopwatch.Elapsed.Milliseconds);
                Console.WriteLine("Indexed items per second {0}", (top / stopwatch.Elapsed.TotalMilliseconds) * 1000);
            }
        }
        private static void DeleteIndex(string server, string index)
        {
            var x = CreateClient(server, index);
            var response = x.DeleteIndex(index);
            response.PrintDump();
        }

        private static void CreateIndex(string server, string index)
        {
            var x = CreateClient(server, index);
            var response = x.CreateIndex(index, c =>
                                       c.NumberOfShards(2)
                                        .NumberOfReplicas(0)
                                        .AddMapping<Item>(m => m.Properties(p => p.String(s => s.Index(FieldIndexOption.analyzed).Name("Title"))
                                                                                  .String(s => s.Index(FieldIndexOption.analyzed).Name("Body"))
                                                                                  .Number(n => n.Name("ItemId"))).TypeName("items"))
                );
            response.PrintDump();
        }

        private static void CreateLargeIndex(string server, string index)
        {
            var x = CreateClient(server, index);
            var response = x.CreateIndex(index, c =>
                                       c.NumberOfShards(2)
                                        .NumberOfReplicas(0)
                                        .Analysis(a =>
                                            a.Tokenizers(an => an.Add("standard", new StandardTokenizer()))
                                             .CharFilters(cf => cf.Add("html_strip", new HtmlStripCharFilter()))
                                             .TokenFilters(tf =>
                                                tf.Add("lowercase", new LowercaseTokenFilter())
                                                  .Add("trim", new TrimTokenFilter())
                                                  .Add("stemmer", new StemmerTokenFilter { Language = "light_swedish" })
                                            )
                                        )
                                        .AddMapping<Item>(m => m.Properties(p =>
                                                                    p.String(s => s.Index(FieldIndexOption.analyzed).Name("MainDescription"))
                                                                     .String(s => s.Index(FieldIndexOption.analyzed).Name("Description"))
                                                                     .String(s => s.Index(FieldIndexOption.not_analyzed).Name("PartNumber"))
                                                                     .Number(n => n.Name("ProductID"))
                                                                     .Number(n => n.Name("ManufacturerPartnerID"))
                                                                     .Number(n => n.Name("Weight"))
                                                                 )
                                                                .TypeName("FSProducts"))
                );
            response.PrintDump();
        }

        private static IEnumerable<Item> CreateItems()
        {
            yield return new Item
                             {
                                 ItemId = 0,
                                 Title = "KLOCKA (NY) PRIMO BY INEX METROPOL REK PRIS 799 SEK",
                                 Body = @"Tillverkare:            Inex of Scandinavia 
Serie:                     Primo
Modell:                  Dam klocka
Urverk:                  Miyota 
Boett:                    Tryck
Urtavla:                 Svart
Glas:                     Mineral  
Armband:             Rostfritt stål
Spänne:                Rostfritt stål
Mått:                     Bredd ca 2.5 cm
Vattenskydd:       30 meter vattentät
Typ:                      Modeklocka 
Garanti:                Kassakvitto gäller som 6 månaders garanti

Passar handleder upp till 20 cm

Samfrakt gäller vid fler köp!

Ta gärna en titt på våra andra auktioner!"
                             };
            yield return new Item
                             {
                                 ItemId = 1,
                                 Title = "Fleecetröja/Windstopper cerise från POP storlek 98",
                                 Body = @"Windstopper/Fleecetröja med dragkedja, cerise, märke Porlarn O. Pyret, storlek 98
Sömmar i form av grå reflexer. Svart nätfoder som ""andas"".
Mycket fint skick, inga fläckar eller hål. Kantbandet på vänster sida är lagat med några stygn längst upp."
                             };
            yield return new Item
                             {
                                 ItemId = 2,
                                 Title = "Rapala Ike's Custom Ink DT-4 / Special Färg",
                                 Body = @"Ni bjuder på 1 st Ike's Rapala som är helt ny i ask/förpackning... 
 
                Längd ca  5 cm + skeden  och väger ca 9 gram /  Floating  
                      Färgen är = Smash  /  SMSH   ..  Går på 1,2 meters djup..
 
 
Betalning sker till mitt Swedbank konto inom 3 dagar efter avslutad auktion.
Obs! Vinnarmailet kommer automatiskt med Traderas mail.
 Jag accepterar även avhämtning."
                             };
        }
    }

    public class ViewItemPage
    {
        public int Id { get; set; }
        public int Ftgnr { get; set; }
        public string Alias { get; set; }
        public string ShortDescription { get; set; }
        public int Category { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
        public decimal ReservePrice { get; set; }
        public decimal ButItNow { get; set; }
        public decimal MaxBid { get; set; }
        public int TotalBids { get; set; }
        public byte HasPicture { get; set; }
        public Int16 Payway { get; set; }
        public byte Shipping { get; set; }
        public byte ExpoFlags { get; set; }
        public char Paypal { get; set; }
        public byte Invoiced { get; set; }
        public DateTime RowCreateDdt { get; set; }
        public byte Xy { get; set; }
        public byte County { get; set; }
        public int ImageId { get; set; }
        public byte Status { get; set; }
        public Int16 ShippingType { get; set; }
        public decimal ShippingCost { get; set; }
        public byte Type { get; set; }
        public int QntStart { get; set; }
        public int QntRemaining { get; set; }
        public Int16 ItemCondition { get; set; }
        public string BestMatch { get; set; }
        public string LongDescription { get; set; }
        public string PostAdress { get; set; }
        public byte RatingPosProc { get; set; }
        public Single DsrAverage { get; set; }
        public Int16 UserType { get; set; }
        public bool HasShopAccount { get; set; }
        public int? SellerShopId { get; set; }
        public Single UsdrAvg { get; set; }
        public int Relevance { get; set; }
        public int CatLvl1 { get; set; }
        public int CatLvl2 { get; set; }
        public int CatLvl3 { get; set; }
        public int CatLvl4 { get; set; }
        public DateTime LastSoldItemDate { get; set; }
        public Int16 SoldCount { get; set; }
        public int SellerBoost { get; set; }
        public DateTime SellerBoostEndDate { get; set; }
        public int Longitude { get; set; }
        public int Latitude { get; set; }
        public bool AcceptsPickup { get; set; }
        public string MainImageFileName { get; set; }
        public int CampaignCodeId { get; set; }
        public int Parent { get; set; }
    }
}
