COVID-19 Impact on US Traffic Accidents
(Team 036-Bavithra Radhakrishnan, Chandra Sekhar Nookarapu, Lohitha Rajasekar, Sravya Karamched, Swaminathan Murugesan, Uday Bag)

Introduction
Automobiles have been in the preeminent position as the primary means of transportation over several decades. It enhances the lives of individuals and the society, but benefits come with a price. Every year on average, 6 million accidents occur and around 3 million people are injured, and more than 90 people die every day in the US. Because of this frequency, traffic accidents are a major cause of death, cutting short millions of lives per year.

Proposed Method
In this project, we aim to build an Interactive User Interface that provides an in-depth analysis and visualization of US Traffic Accidents. The analytical UI shows the accident hotspot locations, casualty analysis, discover patterns, and extract cause and effect rules that measure the impact of precipitation or other environmental stimuli on accident occurrence. We used the recent dataset to study the positive/negative impact of COVID-19 pandemic on traffic accidents. While analyzing traffic accidents and behavior, we explored if the past trends and predictions are valid in the COVID-19 era and find the correlation (if any) between them.


Installation/Setup
The following steps should be followed for installation and setup of our project

1. Download the US Accidents data from https://www.kaggle.com/sobhanmoosavi/us-accidents 
2. Download the COVID data from https://usafacts.org/visualizations/coronavirus-covid-19-spread-map/ 
3. Setup the AWS environment:
a. Create an AWS account
b. Create a new S3 Storage bucket – for storing data
i. In the AWS Management Console click on S3 under All services ? Storage. 
ii.  In the S3 console, click on Create Bucket. Provide a bucket name of your choice
c. EMR setup to run Jupyter notebook 
* Setup the first empty notebook 
* Go to Amazon EMR. Select Notebooks on the right menu. Click Create Notebook
* Give a name to the notebook
* For instance type, select m5.xlarge
* AWS service role, select EMR_Notebooks_DefaultRole
* Notebook location, select the s3 bucket
* Once created, open it up and test it
4. Importing input data:
* Go to S3 and import both US Accidents and COVID data in here
5. Importing Notebook:
* Go to EMR and import the below notebooks
> 1_COVIDCleaning.ipynb: This notebook is used for Data Cleaning/Manipulation of COVID dataset
> 2_AccidentCleaning_CombiningDatasets.ipynb: This notebook is used for Data Cleaning/Manipulation of Traffic accident dataset and for combining both COVID and accident datasets
> 3_KmeansDBScan.ipynb: This notebook is used for experimenting with kmeans/DBSCAN algorithms for creating new columns wherever necessary
> 4_ModelBuilding1.ipynb: This notebook is used for relevant data manipulations required for model building 
> 5_ModelBuilding2.ipynb: This notebook is used to build different models using different ML algorithms

Execution
1. Execute the notebook, build model, and generate required file for visualization: 
* Open the relevant notebook and run the code. 

2. Visualization:
* Go to the following link and view the Introduction page for more details https://public.tableau.com/profile/bavithra.r1051#!/vizhome/USAccident_Covid_data_Analysis/Introduction?publish=yes 
* In case, you want to improve/edit the existing dashboard,
o Download tableau desktop edition and install 
o Open the above link and download the dashboard










