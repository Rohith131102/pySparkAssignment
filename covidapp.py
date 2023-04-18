from pyspark.sql.types import *
from pyspark.sql.functions import *
from flask import Flask, jsonify
from pyspark_assignment import  covidData # Fecthing the dataframe created in other file




app = Flask(__name__)
@app.route('/') 
def home():
    # returning the jsonfied index
    return jsonify({
                    '/getcsvfile':"To export data to csv file at given path",
                    '/most_affected_state': "Most affected state among all the states ( total death/total covid cases)",
                    '/least_affected_state': "Least affected state among all the states ( total death/total covid cases)",
                    '/highest_covid_cases': "State with highest covid cases.",
                    '/least_covid_cases': "State with least covid cases.",
                    '/total_cases': "Total cases.",
                    '/most_efficient_state':"State that handled the covid most efficiently( total recovery/ total covid cases).",
                    '/least_efficient_state': "State that handled the covid least efficiently( total recovery/ total covid cases)."
                    })

@app.route('/get_csvfile') # function to store csv file in Desktop to route on /get_csvfile
def getCsvfile():
    # Exporting the data into csv as single file
    covidData.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/rohith_boodireddy/Desktop/results")
    return jsonify({"Message":"csv stored in Desktop"}) # returning the success meassage jsonfied response


# @covidInda.route('/get_data')
# def printData():
#     return json().loads(response)



@app.route('/most_affected_state')  # function to get most affected state to route on /most_affected_state
def getMostAffectedState():
   #getting max ratio using aggregrate max and then filtering the state which is most affected 
    max_ratio = covidData.select(col("state"), (col("death")/col("confirm")).alias("death_to_confirm_ratio")) \
             .agg(max("death_to_confirm_ratio")).collect()[0][0]

    most_affected_state = covidData.filter((col("death")/col("confirm")) == max_ratio) \
                       .select(col("state")).collect()[0][0]

    return jsonify({'Most Affected State': most_affected_state})  # returning the jsonfied response

@app.route('/least_affected_state')  # function to get least affected state to route on /least_affected_state
def getLeastAffectedState():
    #getting min ratio using aggregrate min and then filtering the state which is least affected 
    min_ratio = covidData.select((col("death")/col("confirm")).alias("death_to_confirm_ratio")) \
              .agg(min("death_to_confirm_ratio")).collect()[0][0]
    least_affected_state = covidData.filter((col("death")/col("confirm")) == min_ratio) \
                           .select(col("state")).collect()[0][0]
    return jsonify({'Least Affected State': least_affected_state}) # returning the jsonfied response

@app.route('/highest_covid_cases') # function to get highest covid cases to route on /highest_covid_cases
def getHighestCovidCases():
   #sorting the df by confirm in Desc and selecting state in top most record
    highest_covid_cases=covidData.sort((covidData.confirm).desc()).select(col("state")).collect()[0][0]
    return jsonify({'Highest Covid Cases':highest_covid_cases}) # returning the jsonfied response

@app.route('/least_covid_cases') # function to get least covid cases to route on /least_covid_cases
def getLeastCovidCases():
    #sorting the df by confirm in Asc and selecting state in top most record
    least_covid_cases=covidData.sort(covidData.confirm).select(col("state")).collect()[0][0]
    return jsonify({'Least Covid Cases':least_covid_cases}) # returning the jsonfied response

@app.route('/total_cases') # function to get total cases to route on /total_cases
def getTotalCases():
    #total cases in india = sum of total cases in all the states.so adding total cases using sum
    total_cases=covidData.select(sum(covidData.total).alias("Total cases")).collect()[0][0]
    return jsonify({'Total Cases':total_cases}) 
    
@app.route('/most_efficient_state')# function to get most efficient state to route on /most_efficient_state
def getMostEfficientState():
    #getting max ratio using aggregrate max and then filtering the state which is most efficient 
    max_ratio = covidData.select((col("cured")/col("confirm")).alias("efficient")).agg(max("efficient")).collect()[0][0]
    most_efficient_state = covidData.filter(col("cured")/col("confirm")==max_ratio).select(col("state")).collect()[0][0]
    return jsonify({'Most Efficient State': most_efficient_state})


@app.route('/least_efficient_state') # function to get least efficient state to route on /least_efficient_state
def getLeastEfficientState():
    #getting min ratio using aggregrate min and then filtering the state which is least efficient 
    min_ratio = covidData.select((col("cured")/col("confirm")).alias("efficient")).agg(min("efficient")).collect()[0][0]
    least_efficient_state = covidData.filter(col("cured")/col("confirm")==min_ratio).select(col("state")).collect()[0][0]
    return jsonify({'Least Efficient State': least_efficient_state})

if __name__ == '__main__':
    app.run(debug=True,port=5000) #running the app on the port 5000
