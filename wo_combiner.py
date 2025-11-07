from pyspark import SparkContext, SparkConf
from datetime import datetime
import time
import sys

def parse_line(line):
    parts = line.strip().split()
    if len(parts)!=5:
        return None
    date,time,action,p_id,r_id=parts
    timestamp=f"{date} {time}"
    if action=="WAIT":
        return ((p_id,r_id),timestamp)
    elif action=="HOLD":
        return ((r_id,p_id),timestamp)
    else: 
        return None

''' Q1.1 RAG edge creation '''
def create_edge(lines):
    return lines.map(parse_line).filter(lambda x:x is not None).distinct()

''' Q1.2 Initialize graph metrics edges_rdd has (src,dst),timestamp '''
def graph_metrics(edges_rdd):
    edges=edges_rdd.map(lambda x:x[0]).distinct().cache() 
    nodes=edges.flatMap(lambda x:x).distinct()
    process_nodes=nodes.filter(lambda n:n.startswith("P")).distinct().count()
    resource_nodes=nodes.filter(lambda n:n.startswith("R")).distinct().count()
    unique_edges=edges.count()

    '''with open("outputs2.txt","w") as outfile:
        outfile.write(f"{process_nodes}\n")
        outfile.write(f"{resource_nodes}\n")
        outfile.write(f"{unique_edges}\n")'''
    return process_nodes,resource_nodes,unique_edges    

''' Q2 Cycle detection algorithm'''
def canonicalize_cycle(cycle_tuple):
    if(len(cycle_tuple)<2):
        return tuple(cycle_tuple)
    core=list(cycle_tuple[:-1])
    n=len(core)
    #generate all possible rotations
    rotations=[]
    for i in range(n):
        r=tuple(core[i:]+core[:i])
        rotations.append(r)   
    min_r=min(rotations)
    return tuple(list(min_r)+[min_r[0]])    

def cycle_detection(edges_rdd, sc, max_len=25):
    # Get edges and timestamps in one pass
    edges_with_ts = edges_rdd.map(lambda x: (x[0], x[1])).cache()
    
    # Get unique edges and latest timestamps
    edges_pairs = edges_with_ts.map(lambda x: x[0]).distinct().cache()
    edge_latest_timestamp = edges_with_ts.reduceByKey(max).collectAsMap()
    edges_with_ts.unpersist()
    
    # Build adjacency list WITHOUT combiner
    adj_list = edges_pairs.groupByKey().mapValues(list).collectAsMap()
    
    # Broadcast adjacency list for efficient lookups
    adj_list_bc = sc.broadcast(adj_list)
    
    # Initialize paths
    paths = edges_pairs.map(lambda x: (x[0], (x[0], x[1]))).cache()
    
    all_cycles = []
    
    for length in range(2, max_len + 1):
        adj = adj_list_bc.value
        
        joined = (
            paths.flatMap(lambda x: [
                (x[1][0], x[1] + (nb,))
                for nb in adj.get(x[1][-1], [])
            ])
            .distinct()
        )
        
        if joined.isEmpty():
            paths.unpersist()
            break
        
        cycles_and_paths = joined.map(lambda x: (x[1][0] == x[1][-1], x[1]))
        
        new_cycles = (
            cycles_and_paths
            .filter(lambda x: x[0])
            .map(lambda x: canonicalize_cycle(x[1]))
            .distinct()
            .collect()
        )
        
        all_cycles.extend(new_cycles)
        
        old_paths = paths
        paths = (
            cycles_and_paths
            .filter(lambda x: not x[0])
            .map(lambda x: (x[1][0], x[1]))
            .cache()
        )
        old_paths.unpersist()
        
        if paths.isEmpty():
            paths.unpersist()
            break
    
    edges_pairs.unpersist()
    adj_list_bc.unpersist()
    
    unique_cycles = list(set(all_cycles))
    sorted_cycles_list = sorted(unique_cycles, key=lambda x: (len(x) - 1, x))
    
    process_involved = set()
    for c in sorted_cycles_list:
        for n in c:
            if n.startswith("P"):
                process_involved.add(n)
    
    return sorted_cycles_list, process_involved, edge_latest_timestamp

def cycle_metrics(sorted_cycles_list,process_involved):
    for c in sorted_cycles_list:
        for n in c:
            if n.startswith("P"):
                process_involved.add(n)    
    '''with open("outputs2.txt","a") as outfile:
        outfile.write(f"{len(sorted_cycles_list)}\n")
        #to print cycles
        for c in sorted_cycles_list:
            if c[0]==c[-1]:
                outfile.write(" ".join(c[:-1])+"\n")
            else:
                outfile.write(" ".join(c)+"\n")   
        outfile.write(f"{len(process_involved)}\n")
        for p in sorted(process_involved,key=lambda x: (int(x[1:])) if x[1:].isdigit() else x):
            outfile.write(p+" ")
        outfile.write(f"\n")'''

''' Q3.1 Deadlock formation timeline '''
def deadlock_timeine(all_cycles,edge_timestamps):
    deadlock_dates={}
    for cycle in all_cycles:
        latest_timestamp=None
        for i in range(len(cycle)-1):
            source=cycle[i]
            destination=cycle[i+1]
            edge=(source, destination)
            if edge in edge_timestamps:
                timestamp=edge_timestamps[edge]
            elif(destination,source) in edge_timestamps:
                timestamp=edge_timestamps[(destination, source)]
            else:
                continue
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
        if latest_timestamp:
            date_only=latest_timestamp.split()[0]
            deadlock_dates[date_only]=deadlock_dates.get(date_only,0)+1

    freq_deadlock_dates=sorted(deadlock_dates.keys())

    '''with open("outputs2.txt","a") as outfile:
        for date in freq_deadlock_dates:
            outfile.write(f"{date} {deadlock_dates[date]}\n")'''

''' Q4 Graph structure analysis '''        
def graph_structure_analysis(edges_list):        
        in_deg={}
        out_deg={}
        out_lines=[]
        edges=edges_list.map(lambda x:x[0]).distinct().collect()
        for source,destination in edges:
            if(destination.startswith('R')):
                in_deg[destination]=in_deg.get(destination,0)+1;
            if(source.startswith('P')):
                out_deg[source]=out_deg.get(source,0)+1;
        
        resources_max_indegree=sorted(in_deg.items(),key=lambda x:(-x[1],x[0]))
        count=0
        for resource,degree in resources_max_indegree:
            if(count>=5):
                break
            out_lines.append(f"{resource} {degree}")
            count+=1

        process_max_outdegree=sorted(out_deg.items(),key=lambda x:(-x[1],x[0]))
        count=0
        for process,degree in process_max_outdegree:
            if(count>=5):
                break
            out_lines.append(f"{process} {degree}") 
            count+=1   

        '''with open("outputs2.txt","a") as outfile:
            for line in out_lines:
                outfile.write(line+"\n")'''

def main(log_file,cores=None):
    conf=SparkConf().setAppName("RAG_Deadlock_Analysis_NO_COMBINER")
    if cores:
        conf=conf.setMaster(f"local[{cores}]")
    else:
        conf=conf.setMaster(f"local[*]")   
    sc=SparkContext(conf=conf)

    try:
        lines=sc.textFile(log_file)
        edges_rdd=create_edge(lines).distinct()
        process_nodes,resource_nodes,unique_edges=graph_metrics(edges_rdd)

        start_time=time.time()
        cycles,processes,edge_timestamp_map=cycle_detection(edges_rdd,sc)
        end_time=time.time()
        execution_time=end_time-start_time
        print(execution_time)
        '''with open("outputs2.txt","a") as outfile:
            outfile.write(f"{execution_time:.2f}\n")'''
        cycle_metrics(cycles,processes)    
        if cycles:
            deadlock_timeine(cycles,edge_timestamp_map)
        graph_structure_analysis(edges_rdd)
    finally:
        sc.stop()            
    

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python without_combiner.py <log_file_path> [num_cores]")
        sys.exit(1)
    log_file = sys.argv[1]
    cores = int(sys.argv[2]) if len(sys.argv) > 2 else None
    main(log_file,cores)