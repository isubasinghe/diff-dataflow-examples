use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection};
use graphs::{Iter, Node, Edge};
use timely::dataflow::Scope;
use timely::dataflow::operators::{ToStream, Map};
use differential_dataflow::input::Input;
use differential_dataflow::operators::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::consolidate::Consolidate;

fn main() {

    timely::execute_from_args(std::env::args(), | worker| {
        let filename = std::env::args().nth(1).expect("Must supply filename");
        let steps = std::env::args().nth(2)
            .expect("Must supply number of steps")
            .parse::<Node>().expect("Could not parse steps");
        
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

        let mut goals = 
        worker.dataflow(|scope| {

            let timer = timer.clone();

            let edges = edges
                .to_stream(scope)
                .map(|edge| (edge, 0, 1))
                .as_collection();
            
            let (handle, goals) = scope.new_collection();

            differential_dataflow::algorithms::graphs::bijkstra::bidijkstra(&edges, &goals)
                .filter(move |_| inspect)
                .map(|(node, _)| node)
                .consolidate()
                .inspect(move |x| print!("{:?}\tGoals:{:?}", timer.elapsed(), x))
                .probe_with(&mut probe);

            handle
        });

        goals.advance_to(1);
        goals.flush();

        while probe.less_than(goals.time()) {
            worker.step();
        }

        println!("{:?}\tComputation stable", timer.elapsed());

        goals.insert((source, steps));

        goals.advance_to(2);
        goals.flush();

        while probe.less_than(goals.time()) {
            worker.step();
        }
        
        println!("{:?}\tComputation complete", timer.elapsed());

    }).expect("Failed to start timely");

}