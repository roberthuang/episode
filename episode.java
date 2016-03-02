import java.io.*;
import java.util.*;
public class episode {
    public ArrayList<ArrayList<ArrayList<String>>> result = new ArrayList<>();
    // writer to write output file
    static BufferedWriter writer = null;
    public static void main(String[] args) throws FileNotFoundException {
        int window_size = 2;
        int min_sup = 20;
        //Run
        episode e = new episode();
        e.runAlgo(window_size, min_sup);   

    }
    
    void runAlgo(int window_size, int min_sup) throws FileNotFoundException {
        ArrayList<ArrayList<String>> CES = TransformToCES("transformed_petro_subset1_feature.csv");
        try {
            writer = new BufferedWriter(new FileWriter("episodes.txt")); 
            //Debug 
            int i = 1;
            if (i == 0) {
                for (ArrayList<String> events : CES) {
                    System.out.println(events);
                }
            }
            ArrayList<ArrayList<ArrayList<String>>> F1 = ScanF1(CES, min_sup);
            HashMap<ArrayList<ArrayList<String>>, Integer> number = new HashMap<>();
            int weight = 1;
            for (ArrayList<ArrayList<String>> f1 : F1) {
                number.put(f1, weight);
                weight++; 
            }
            for (ArrayList<ArrayList<String>> f1 : F1) {
                SerialJoin(f1, f1, F1, CES, min_sup, window_size, number);
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
     * Find frequent 1 episode F1 
     * SE: Simultaneous Event Sets
     * E: Event   
     */  
    private ArrayList<ArrayList<ArrayList<String>>> ScanF1 (ArrayList<ArrayList<String>> CES, int min_sup) {
        ArrayList<ArrayList<ArrayList<String>>> result = new ArrayList<>();   
        HashMap <String, Integer> set = new HashMap<>();
        for (ArrayList<String> SE : CES) {
            for (String E : SE) {
                if (set.get(E) == null) {
                    int count = 1;
                    set.put(E, count);
                } else {
                    int count = set.get(E);
                    count++;
                    set.put(E, count);
                }
            }
        }
        
        for (String E : set.keySet()) {
            if (set.get(E) >= min_sup) {
                ArrayList<ArrayList<String>> SE = new ArrayList<>();
                ArrayList<String> Sub_SE = new ArrayList<>();;
                Sub_SE.add(E);
                SE.add(Sub_SE);
                result.add(SE);
            }
        }
        return result;
    }
        
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
    
    private void SerialJoin(ArrayList<ArrayList<String>> alpha, ArrayList<ArrayList<String>> lastItem, ArrayList<ArrayList<ArrayList<String>>> F1, ArrayList<ArrayList<String>> CES, int min_sup, int window_size, HashMap<ArrayList<ArrayList<String>>, Integer> number) {
        for (ArrayList<ArrayList<String>> f_j : F1) {
            //for (ArrayList<ArrayList<String>> fk : F1) {
            //    System.out.print(fk + " ");
            //}
            //System.out.println();
            if (number.get(f_j) > number.get(lastItem)) {
                ArrayList<ArrayList<String>> equal_join = equalJoin(alpha, f_j); 
                int sup = ScanCES(CES, equal_join, window_size);       
                if (sup >= min_sup) {
                    //OUTPUT RESULT
                    //e.add_frequent_episode(equal_join);
                    System.out.println("equal: "  + equal_join);
                    try {
                        saveEpisode(equal_join, sup);
                    } catch (IOException e) {
                        System.out.println("IOException!");
                    }
                    SerialJoin(equal_join, f_j, F1, CES, min_sup, window_size, number);
                }
            } 
            
            ArrayList<ArrayList<String>> temporal_join = temporalJoin(alpha, f_j); 
            int sup = ScanCES(CES, temporal_join, window_size);  
            if (sup >= min_sup) {
                    System.out.println("temporaljoin: " + temporal_join);
                    //OUTPUT RESULT
                    //e.add_frequent_episode(temporal_join);
            //        System.out.println("temp: "  + temporal_join);
                    try {
                        saveEpisode(temporal_join, sup);
                    } catch (IOException e) {
                        System.out.println("IOException!");
                    }
                    SerialJoin(temporal_join, f_j, F1, CES, min_sup, window_size, number);
            }
            
        }
    
    }
    
    ArrayList<ArrayList<String>> equalJoin(ArrayList<ArrayList<String>> alpha, ArrayList<ArrayList<String>> f_j) {
        ArrayList<ArrayList<String>> join = new ArrayList<>();
        for (int i = 0; i < alpha.size(); i++) {
            ArrayList<String> temp = new ArrayList<>();
            if (i == alpha.size()-1) {
                ArrayList<String> subset = alpha.get(alpha.size()-1);
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
    
    ArrayList<ArrayList<String>> temporalJoin(ArrayList<ArrayList<String>> alpha, ArrayList<ArrayList<String>> f_j) {
        ArrayList<ArrayList<String>> join = new ArrayList<>();
        for (int i = 0; i < alpha.size(); i++) {
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
        //System.out.println("alpha:"  + alpha + "xxx" + join + "xxx" + f_j);
        return join;
    }
    
    int ScanCES(ArrayList<ArrayList<String>> CES, ArrayList<ArrayList<String>> join, int window_size) {
        int sup = 0;
        for (int i = 0; i <= CES.size()-window_size; i++) {
            int start = i;
            int end = start + window_size-1;
            //MATCH
            ArrayList<ArrayList<String>> SubSequence = GetSubS(CES, start, end);
            int size = 0;
            int current = 0;
            for (int j = 0; j < SubSequence.size(); j++) {
                for (int k = current; k < join.size(); k++) {
                    if (SubSequence.get(j).containsAll(join.get(k))) {
                        current = k;
                        size++;
                    }
                    break;
                }
            
            }
            if (size == join.size()) {
                sup++;
            }
        }
        return sup;
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
    
    public void add_frequent_episode(ArrayList<ArrayList<String>> equal_join) {
        result.add(equal_join);
    }
    
    private void saveEpisode(ArrayList<ArrayList<String>> frequentEpisode, int sup) throws IOException {
                
        // if the result should be saved to a file
	if(writer != null){
	    StringBuilder r = new StringBuilder("");
	    for(ArrayList<String> episode : frequentEpisode){
	        for(String event : episode){
		    String string = event.toString();
		    r.append(string);
		    r.append(' ');
	        }
		r.append("-1 ");
	    }
	    r.append(" #SUP: ");
	    r.append(sup);
	    writer.write(r.toString());
	    writer.newLine();
	} else{
	    System.out.println("Error!");
	}
    }
}
