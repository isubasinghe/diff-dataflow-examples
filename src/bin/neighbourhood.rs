use graphs::{Iter, Node};
use timely::dataflow::operators::ToStream;

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
            let graph = 
            edges
                .to_stream(scope)
                .map(|edge| (edge, 0, 1) )
                .as_collection();
            
            let (handle, source) = scope.new_collection();

            let source = source.map(|(node, steps)| (node, (node, steps)));

            source.iterate(|inner| {
                let edges = edges.enter(&inner.scope());
                let souce = source.enter(&inner.scope());

                inner
                    .filter(|node, steps|  steps > 0)
                    .join_map(&edges, |&node, &steps, &dest| (dest, (node, steps-1)))
                    .concat(&source)
                    .distinct()
                    .filter(move |_| inspect)
                    .map(|(node, )| node)
                    .consolidate()
                    .inspect(|x| println!("{:?}\t{}", timer.elapsed(), x))
                    .probe_with(&mut probe);
            
            });
            handle
        });

        sources.advance_to(1);
        sources.flush();

        while probe.less_than(sources.time()) {
            worker.step();
        }

        println!("{:?}\tComputation stable", timer.elapsed());

        sources.insert(source);
        sources.advance_to(2);
        sources.flush();
        
        
        while probe.less_than(sources.time()) {
            worker.step();
        }

        println!("{:?}\tQuery completed", timer.elapsed());


    }).expect("Timely failed to start");
}