use differential_dataflow::AsCollection;
use graphs::{Iter, Node};
use timely::dataflow::operators::{ToStream, Map};
use differential_dataflow::input::Input;
use differential_dataflow::operators::Iterate;
use timely::dataflow::operators::Enter;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::collection::Collection;
use differential_dataflow::operators::consolidate::Consolidate;
fn main() {

    timely::execute_from_args(std::env::args(), |worker| {
        let filename = std::env::args().nth(1).expect("Must supply filename");
        let steps = std::env::args().nth(2)
            .expect("Must supply number of steps")
            .parse::<Iter>().expect("Could not parse steps");
        
        let source = std::env::args().nth(3)
            .expect("Must supply source")
            .parse::<Node>().expect("Could not parse source");

        let inspect = std::env::args().nth(4)
            .expect("Must supply inspect")
            .parse::<bool>().expect("Could not parse bool");
        
        println!("\nFILE: {}\n", filename);

        let index = worker.index();
        let peers = worker.peers();

        let timer = worker.timer();

        let edges = graphs::load_graph(&filename, index, peers);

        println!("{:?}\t Loaded {} edges", timer.elapsed(), edges.len());

        let sources = worker.dataflow(|scope| {
            let edges = edges
            .to_stream(scope)
            .map(|edge| (edge, 0, 1))
            .as_collection();
            

            let (handle, source) = scope.new_collection();
            
            source
                .iterate(|inner| {
                    let edges = edges.enter(&inner.scope());
                    inner
                    .filter(|(node, (root, steps))| steps > 0)
                    .join_map(&edges, |&node, &steps, &dest| (dest, (node, steps-1)))
                    .concat()
                })

            handle



        }) 
    }).expect("Timely failed to start");
}