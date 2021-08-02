use differential_dataflow::AsCollection;
use graphs::{Iter, Node};
use timely::dataflow::operators::{ToStream, Map};
use differential_dataflow::input::Input;
use differential_dataflow::operators::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Threshold;
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

        let mut probe = timely::dataflow::ProbeHandle::new();

        let mut sources = worker.dataflow(|scope| {
            let timer = timer.clone();
            let edges = edges
            .to_stream(scope)
            .map(|edge| (edge, 0, 1))
            .as_collection();
            

            let (handle, source) = scope.new_collection();
            
            let source = source.map(|(node, steps)| (node, (node, steps)));

            let neighbours = 
            source
                .iterate(|inner| {
                    let edges = edges.enter(&inner.scope());
                    let source = source.enter(&inner.scope());

                    inner
                    .filter(|(_node, (_root, steps))| steps > &0)
                    .join_map(&edges, |&_node, &(source, steps), &dest| (dest, (source, steps-1)))
                    .concat(&source)
                    .consolidate()
                    .distinct()
                })
                .map(|(dest, (node, _))| (node, dest))
                .distinct()
                .filter(move |_| inspect)
                .map(|(node, _)| node)
                .consolidate()
                .inspect(move | (x, _, _)| println!("{:?}\t{}", timer.elapsed(), x))
                .probe_with(&mut probe);
            handle
            
        });

        sources.advance_to(1);
        sources.flush();


        while probe.less_than(sources.time()) {
            worker.step();
        }

        println!("{:?} Computation stable", timer.elapsed());

        sources.insert((source, steps));
        sources.advance_to(2);
        sources.flush();

        while probe.less_than(sources.time()) {
            worker.step();
        }

        println!("{:?} Computation completed", timer.elapsed());

    }).expect("Timely failed to start");
}