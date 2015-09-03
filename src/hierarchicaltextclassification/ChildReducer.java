package hierarchicaltextclassification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;


import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




	public class ChildReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//String a = "4597, 0  , 260304,  126336, 109467 586:1 701:1 712:1 715:1 776:1 1144:1 1865:1 4154:1 10306:1 25109:1 30715:1 33456:1 94508:1 257173:1"; 

			ArrayList<String> train = new ArrayList<String>();
			ArrayList<String> test = new ArrayList<String>();


			String line="";
			boolean classifiedTest=true;
			for(Text val : values){
				line = val.toString();
				if(line.contains("Training")) classifiedTest=false;
			}//for ends
			
			if(classifiedTest) {
				for(Text val : values){
					context.write(key, new Text(val.toString()+" Processed"));
				}
			}

			svm_parameter param=new svm_parameter();
			param.svm_type=svm_parameter.C_SVC;
			param.kernel_type=svm_parameter.RBF;
			param.gamma=0.5;
			param.nu=0.5;
			param.cache_size=20000;
			param.C=1;
			param.eps=0.001;
			param.p=0.1;


			HashMap<Integer, HashMap<Integer, Double>> featuresTraining=new HashMap<Integer, HashMap<Integer, Double>>();
			HashMap<Integer, Integer> labelTraining=new HashMap<Integer, Integer>();
			HashMap<Integer, HashMap<Integer, Double>> featuresTesting=new HashMap<Integer, HashMap<Integer, Double>>();

			HashSet<Integer> features=new HashSet<Integer>();
			//Read in training data



			try{
				int lineNum=0;
				for( String trainingLine : train ){
					featuresTraining.put(lineNum, new HashMap<Integer,Double>());
					String[] tokens=trainingLine.split("\\s+");
					int label=Integer.parseInt(tokens[0]);
					labelTraining.put(lineNum, label);
					for(int i=1;i<tokens.length;i++){
						String[] fields=tokens[i].split(":");
						int featureId=Integer.parseInt(fields[0]);
						double featureValue=Double.parseDouble(fields[1]);
						features.add(featureId);
						featuresTraining.get(lineNum).put(featureId, featureValue);
					}
					lineNum++;
				}
			}catch (Exception e){
				//some chutiyaap stacktrace
			}

			//System.out.println("features hash set is +\t"+featuresTraining);


			try{

				//String line=null;
				int lineNum=0;
				for(String testLine : test){
					featuresTesting.put(lineNum, new HashMap<Integer,Double>());
					String[] tokens=testLine.split("\\s+");
					for(int i=1; i<tokens.length;i++){
						String[] fields=tokens[i].split(":");
						int featureId=Integer.parseInt(fields[0]);
						double featureValue=Double.parseDouble(fields[1]);
						featuresTesting.get(lineNum).put(featureId, featureValue);
					}
					lineNum++;
				}

			}catch (Exception e){
				//code for test data
			}


			svm_problem prob=new svm_problem();
			int numTrainingInstances=featuresTraining.keySet().size();
			prob.l=numTrainingInstances;
			prob.y=new double[prob.l];
			prob.x=new svm_node[prob.l][];

			
			for(int i=0;i<numTrainingInstances;i++){
				HashMap<Integer,Double> tmp=featuresTraining.get(i);

				prob.x[i]=new svm_node[tmp.keySet().size()];
				int indx=0;
				for(Integer id:tmp.keySet()){
					svm_node node=new svm_node();
					node.index=id;
					node.value=tmp.get(id);
					prob.x[i][indx]=node;
					indx++;
				}

				prob.y[i]=labelTraining.get(i);

			}

			svm_model model=svm.svm_train(prob,param);
			//System.out.println("feature test is" + featuresTesting);


			int testIndex=0;
			for(Integer testInstance:featuresTesting.keySet()){
				
				String[] testLine=test.get(testIndex).split("\\s+");
				String testFeatureVector="";
				for(int i=1;i<testLine.length;i++){
					if(i==1){
						testFeatureVector=testLine[i];
					}
					else{
						testFeatureVector=testFeatureVector+" "+testLine[i];
					}
				}
				HashMap<Integer, Double> tmp=new HashMap<Integer, Double>();
				int numFeatures=tmp.keySet().size();
				svm_node[] x=new svm_node[numFeatures];
				int featureIndx=0;
				for(Integer feature:tmp.keySet()){
					x[featureIndx]=new svm_node();
					x[featureIndx].index=feature;
					x[featureIndx].value=tmp.get(feature);
					featureIndx++;
				}

				double d=svm.svm_predict(model, x);

				//Writing Classified Test Data

				context.write(key,new Text(d+" "+testFeatureVector+"Test"));
				testIndex++;
			}


		}//reduce function ends





	}

