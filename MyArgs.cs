using PowerArgs;

namespace ConsoleApplication1
{
    [TabCompletion]
    [ArgExample("ElasticTest -s http://192.168.1.162:9200 -a Index", "Shows how to use the shortcut version of the switch parameter")]
    public class MyArgs
    {
        [ArgShortcut("s")]
        [ArgDescription("Elasticsearch server")]
        [ArgRequired(PromptIfMissing = false)]
        public string Server { get; set; }

        [ArgShortcut("a")]
        [ArgDescription("Action to execute")]
        [ArgRequired(PromptIfMissing = false)]
        public Action Action { get; set; }

        [ArgShortcut("i")]
        [ArgDescription("Index")]
        public string Index { get; set; }

        [ArgDescription("Shows the help documentation")]
        public bool Help { get; set; }
    }
}