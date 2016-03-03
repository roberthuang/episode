import java.io.*;
import java.util.*;
public class episode {
    // writer to write output file
    static BufferedWriter writer = null;
    public static void main(String[] args) throws FileNotFoundException {
        int window_size = 2;
        double min_sup = 20;
        //Run
        episode e = new episode();
        e.runAlgo(window_size, min_sup);   
    }
    
    public void runAlgo(int window_size, double min_sup) throws FileNotFoundException {
        ArrayList<ArrayList<String>> CES = TransformToCES("transformed_petro_subset1_feature.csv");
        int traing_data_size = CES.size()-window_size;
        try {
            writer = new BufferedWriter(new FileWriter("episodes.txt")); 
            /**First: Scan F1 with target**/
            ArrayList<ArrayList<ArrayList<String>>> F1_with_targets = ScanF1_include_target(CES, min_sup, window_size);
            //for (ArrayList<ArrayList<String>> F1_with_target : F1_with_targets) {
            //	System.out.println(F1_with_target);            
            //}
            int weight = 1;
            HashMap<ArrayList<ArrayList<String>>, Integer> order = new HashMap<>();
            ArrayList<ArrayList<ArrayList<String>>> F1 = new ArrayList<>();
            for (ArrayList<ArrayList<String>> F1_with_target : F1_with_targets) {
                ArrayList<String> fre1 = F1_with_target.get(0);
                ArrayList<ArrayList<String>> temp = new ArrayList<>();
                temp.add(fre1);
                //System.out.println(temp);
                if (order.get(temp) == null) {
                    order.put(temp, weight);
                    F1.add(temp);
                    weight++;
                } else {
                    continue;
                }	            
            }
            //for (ArrayList<ArrayList<String>> f : order.keySet()) {
            //    System.out.println(f  + " " + order.get(f));      
            //}
            //for (ArrayList<ArrayList<String>> f : F1) {
            //    System.out.println(f);      
            //}
            

            for (ArrayList<ArrayList<String>> F1_with_target : F1_with_targets) {
                ArrayList<ArrayList<String>> lastitem = new ArrayList<>();
                lastitem.add(F1_with_target.get(0));
                SerialJoin(F1_with_target, lastitem, F1, CES, min_sup, window_size, traing_data_size, order);
                //System.out.println(f1);
            }
        
            if(writer != null){
	        writer.close();
	    }
	} catch (IOException e) {
            System.out.println("IOException!");
        }
    }
     
    /**
     * Find frequent 1 episode F1 including target
     * SE: Simultaneous Event Sets
     * E: Event   
     */  
    private ArrayList<ArrayList<ArrayList<String>>> ScanF1_include_target(ArrayList<ArrayList<String>> CES, double min_sup, int window_size) {
        //for (ArrayList<String> c : CES) {
        
        //    System.out.println(c);
        //}
        ArrayList<ArrayList<ArrayList<String>>> result = new ArrayList<>();   
        HashMap <ArrayList<ArrayList<String>>, Integer> set = new HashMap<>();
        for (int i = 0; i <= CES.size()-window_size-1; i++) {
            ArrayList<String> SE = CES.get(i);
            //    System.out.println(SE);
            for (int j = 0; j < SE.size()-1; j++) {
                ArrayList<ArrayList<String>> new_SE = new ArrayList<>();
                String event = SE.get(j);
                //System.out.println(event);
                //F1
                ArrayList<String> event1 = new ArrayList<>();
                event1.add(event);
                
                //Target
                ArrayList<String> event2 = new ArrayList<>();
                ArrayList<String> SE_with_target = CES.get(i+window_size);
                String target =  SE_with_target.get(SE_with_target.size()-1);                
                event2.add(target);
                
                //F1 including target
                new_SE.add(event1);
                new_SE.add(event2);
                if (set.get(new_SE) == null) {
                    int count = 1;
                    set.put(new_SE, count);
                } else {
                    int count = set.get(new_SE);
                    count++;
                    set.put(new_SE, count);
                }             
            
            }        
        }
        
        for (ArrayList<ArrayList<String>> SE : set.keySet()) {
        //    System.out.println("HASH: " + SE);
            if (set.get(SE) >= min_sup) {
                result.add(SE);
            }        
        }
        
        return result;
    }
    
    /**
     * Transform to complex event sequence
     * 
     *   
     */      
    private ArrayList<ArrayList<String>> TransformToCES(String fullpath) throws FileNotFoundException{
        ArrayList<ArrayList<String>> records = new ArrayList<>();
	    File inputFile = new File(fullpath);
	    Scanner scl = new Scanner(inputFile);
	    while(scl.hasNextLine()){
		    ArrayList<String> newRecord = new ArrayList<>();
		    String[] tokens = scl.nextLine().split(",");
		    for(String token : tokens){
			    newRecord.add(token);
		    }
		    records.add(newRecord);
	    }
	    scl.close();		
	    return records; 
    }
    
    
    /**
     * Serial Join
     * 
     *   
     */  
    private void SerialJoin(ArrayList<ArrayList<String>> alpha, ArrayList<ArrayList<String>> lastItem, ArrayList<ArrayList<ArrayList<String>>> F1, ArrayList<ArrayList<String>> CES, double min_sup, int window_size, int traing_data_size, HashMap<ArrayList<ArrayList<String>>, Integer> order) {
        for (ArrayList<ArrayList<String>> f_j : F1) {
            //for (ArrayList<ArrayList<String>> fk : F1) {
            //    System.out.print(fk + " ");
            //}
            //System.out.println();
            if (order.get(f_j) >  order.get(lastItem)) {
                ArrayList<ArrayList<String>> equal_join = equalJoin(alpha, f_j); 
                //System.out.println(equal_join);    
                ArrayList<Double> sup_cof = new ArrayList<>();            
                sup_cof = ScanCES(CES, equal_join, window_size);  
                double sup = sup_cof.get(0);   
                double conf = sup_cof.get(1);                      
                if (sup >= min_sup) {
                    //OUTPUT RESULT
                    //e.add_frequent_episode(equal_join);
                    //System.out.println("equal: "  + equal_join);
                    try {
                        double sup_percentage = sup / (double) traing_data_size;
                        //System.out.println(sup + " " + traing_data_size + " " + sup_percentage);
                        saveEpisode(equal_join, sup_percentage, conf);
                    } catch (IOException e) {
                        System.out.println("IOException!");
                    }
                    SerialJoin(equal_join, f_j, F1, CES, min_sup, window_size, traing_data_size, order);
                }
            } 
            
            ArrayList<ArrayList<String>> temporal_join = temporalJoin(alpha, f_j); 
            ArrayList<Double> sup_cof = new ArrayList<>();   
            sup_cof = ScanCES(CES, temporal_join, window_size);  
            double sup = sup_cof.get(0);              
            double conf = sup_cof.get(1);  
            if (sup >= min_sup) {
                    //System.out.println("temporaljoin: " + temporal_join);
                    //OUTPUT RESULT
                    //e.add_frequent_episode(temporal_join);
            //        System.out.println("temp: "  + temporal_join);
                    try {
                    	double sup_percentage = sup / (double) traing_data_size;
                        saveEpisode(temporal_join, sup_percentage, conf);
                    } catch (IOException e) {
                        System.out.println("IOException!");
                    }
                    SerialJoin(temporal_join, f_j, F1, CES, min_sup, window_size, traing_data_size, order);
            }
            
        }
    
    }
    
    /**
     * Equal Join
     * Ex. <A,Clas> and <C> -> <AC, Class>
     *   
     */  
    ArrayList<ArrayList<String>> equalJoin(ArrayList<ArrayList<String>> alpha, ArrayList<ArrayList<String>> f_j) {
        ArrayList<ArrayList<String>> join = new ArrayList<>();
        for (int i = 0; i < alpha.size(); i++) {
            ArrayList<String> temp = new ArrayList<>();
            if (i == alpha.size()-2) {
                ArrayList<String> subset = alpha.get(i);
                for (int j = 0; j < subset.size(); j++) {
                    temp.add(subset.get(j));
                }
                //EQUALJOIN
                ArrayList<String> last_f_j = f_j.get(f_j.size()-1);
                temp.add(last_f_j.get(0));
            } else {
                ArrayList<String> subset = alpha.get(i);
                for (int j = 0; j < subset.size(); j++) {
                    temp.add(subset.get(j));
                }
            }
            join.add(temp);
        }
//      System.out.println("alpha:"  + alpha + "xxx" + join + "xxx" + f_j);
        return join;  
    }
    
    /**
     * Temporal Join
     * Ex. <A, Class> and <C> -> <A,C,Class>
     *   
     */      
    ArrayList<ArrayList<String>> temporalJoin(ArrayList<ArrayList<String>> alpha, ArrayList<ArrayList<String>> f_j) {
        ArrayList<ArrayList<String>> join = new ArrayList<>();
        for (int i = 0; i < alpha.size()-1; i++) {
            ArrayList<String> temp = new ArrayList<>();
            ArrayList<String> subset = alpha.get(i);
            for (int j = 0; j < subset.size(); j++) {
                temp.add(subset.get(j));
            }
            join.add(temp);
        } 
        ArrayList<String> last_temp = new ArrayList<>();
        ArrayList<String> last_f_j = f_j.get(f_j.size()-1);
        for (int i = 0; i < last_f_j.size(); i++) {
            last_temp.add(last_f_j.get(i));
        }
        join.add(last_temp);
        //Add Class
        ArrayList<String> temp = new ArrayList<>();
        temp.add(alpha.get(alpha.size()-1).get(0));
        join.add(temp);
        //System.out.println("alpha:"  + alpha + "xxx" + join + "xxx" + f_j);
        return join;
    }
    
    /**
     * Count spport count
     *   
     */ 
    ArrayList<Double> ScanCES(ArrayList<ArrayList<String>> CES, ArrayList<ArrayList<String>> join, int window_size) {
        double sup = 0;
        double prefix = 0;
        ArrayList<Double> result = new ArrayList<>();
        //System.out.println(CES.size()-window_size);
        for (int i = 0; i <= CES.size()-window_size-1; i++) {
            int start = i;
            int end = start + window_size;
            //MATCH
            ArrayList<ArrayList<String>> SubSequence = GetSubS(CES, start, end);
            int size = 0;
            int current = 0;
            for (int j = 0; j < SubSequence.size(); j++) {
                for (int k = current; k < join.size(); k++) {
                    if (SubSequence.get(j).containsAll(join.get(k))) {
                        current = k;
                        current++;
                        size++;
                        //Count prefix support count
                        if (size == join.size()-1) {
                            prefix++;          
                        }
                        
                    }
                    break;
                }
            
            }
            if (size == join.size()) {
                sup++;      
//                System.out.println("Join: " + join + "   Training: " + SubSequence + " "  + start + " " + end);          
            }
        }
        result.add(sup);
        double confidence = sup / prefix;
        result.add(confidence);
        return result;
    }
    
    ArrayList<ArrayList<String>> GetSubS(ArrayList<ArrayList<String>> CES, int start, int end) {
         ArrayList<ArrayList<String>> result = new ArrayList<>();
         for (int i = start; i <= end; i++) {
             ArrayList<String> SE_result = new ArrayList<>(); 
             ArrayList<String> SE = CES.get(i);
             for (String E : SE) {
                 SE_result.add(E);
             }   
             result.add(SE_result);
         }
         return result;
    }
    
    /**
     * Save frequent episode
     * 
     *   
     */     
    private void saveEpisode(ArrayList<ArrayList<String>> frequentEpisode, double sup, double conf) throws IOException {                
        // if the result should be saved to a file
	if(writer != null){
	    StringBuilder r = new StringBuilder("");
	    for(int i = 0; i < frequentEpisode.size() - 1; i++){
	    	ArrayList<String> episode = frequentEpisode.get(i);
	        for(String event : episode){
		    String string = event.toString();
		    r.append(string);
		    r.append(' ');
	        }
		r.append("-1 ");
	    }
	    r.append("-> ");
	    String Class = frequentEpisode.get(frequentEpisode.size()-1).get(0);
	    String string = Class.toString();
	    r.append(string);
	    r.append("	:	");
	    r.append(sup);
	    r.append(",	");
	    r.append(conf);
	    writer.write(r.toString());
	    writer.newLine();
	} else{
	    System.out.println("Error!");
	}
    }
}
