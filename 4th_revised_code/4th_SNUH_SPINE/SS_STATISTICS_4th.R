library('rJava')
#library("RPostgreSQL")
library('plyr')
library('dplyr')
library('SqlRender')
library('DBI')
library('lubridate')
library('stringr')
library('tidyr')
library('readr')
library("RJDBC")
library("data.table")
library("curl")
library("tableone")

# Hospital information
hname="SNUH"


# set work directory
wd <- getwd() 

if (str_sub(wd,-17,-1)=="SNUH_SPINE_result"){print(wd)} else {
  result_path <- paste0(wd,"/SNUH_SPINE_result")
  if (dir.exists(result_path)==FALSE){dir.create(result_path)}
  setwd(result_path)
}
##########################################DATA#######################################################################
pt_pre1 <- read_csv("./SS/SS_pre_pt1.csv") #6842
pt_pre2 <- read_csv("./SS/SS_pre_pt2.csv")
pt_post1 <- read_csv("./SS/SS_post_pt1.csv")
pt_post2 <- read_csv("./SS/SS_post_pt2.csv")
##########################################TEST#######################################################################
pt_pre1$period <- rep("PRE",nrow(pt_pre1))
pt_post1$period <- rep("POST",nrow(pt_post1))
pt_pre2$period <- rep("PRE",nrow(pt_pre2))
pt_post2$period <- rep("POST",nrow(pt_post2))

pt_pre1 %>% nrow()
pt_pre2 %>% nrow()

pt_visit <- rbind(pt_pre1,pt_post1)
pt_drug <- rbind(pt_pre2,pt_post2)

names(pt_pre1)
names(pt_post1)

######################################################################################################################
# 그룹별 방문 횟수, 평균 비용
######################################################################################################################
names(pt_visit)
pt_visit %>% nrow()
colSums(is.na(pt_visit))
names(pt_visit)[names(pt_visit)=="gender_source_value"] <- "gender"
names(pt_visit)[names(pt_visit)=="diab_final"] <- "diabetes"
# 실험용 
#pt_visit$visit_total_paid <- sample(x=1:30,size=nrow(pt_visit),replace=T)  

######################################Baseline Characteristics###############################
myVars = c("gender","age_cat","dx_location","diabetes","corona","region","period")
catVars = c("gender","age_cat","dx_location","diabetes","corona","region","period")
t1 <- CreateTableOne(data=pt_visit,vars = myVars, factorVars = catVars,strata="period")
table1 <- print(t1,showAllLevels=T)
write.csv(table1, file = "./SS/ss_baseline_characteristics.csv")


sink("./SS/SS_visit_ttest.txt")
print("PRE와 POST 두 시기의 전체 평균 비교")
var.test(pt_visit$visit_total_paid[pt_visit$period=="PRE"],pt_visit$visit_total_paid[pt_visit$period=="POST"])
t.test(pt_visit$visit_total_paid[pt_visit$period=="PRE"],pt_visit$visit_total_paid[pt_visit$period=="POST"],var.equal = T)
t.test(pt_visit$visit_total_paid[pt_visit$period=="PRE"],pt_visit$visit_total_paid[pt_visit$period=="POST"],var.equal = F)

var.test(pt_visit$paid_by_payer[pt_visit$period=="PRE"],pt_visit$paid_by_payer[pt_visit$period=="POST"])
t.test(pt_visit$paid_by_payer[pt_visit$period=="PRE"],pt_visit$paid_by_payer[pt_visit$period=="POST"],var.equal = T)
t.test(pt_visit$paid_by_payer[pt_visit$period=="PRE"],pt_visit$paid_by_payer[pt_visit$period=="POST"],var.equal = F)

var.test(pt_visit$paid_by_patient[pt_visit$period=="PRE"],pt_visit$paid_by_patient[pt_visit$period=="POST"])
t.test(pt_visit$paid_by_patient[pt_visit$period=="PRE"],pt_visit$paid_by_patient[pt_visit$period=="POST"],var.equal = T)
t.test(pt_visit$paid_by_patient[pt_visit$period=="PRE"],pt_visit$paid_by_patient[pt_visit$period=="POST"],var.equal = F)
sink()

result <- pt_visit %>% group_by(dx_location,gender,age_cat,region,diab_final,corona,oriental,total_type) %>% 
  dplyr::summarise(n=n(),
                   n_pt = length(unique(person_id)),
                   cost_visit_sum=sum(visit_total_paid),
                   cost_payer_sum = sum(paid_by_payer),
                   cost_patient_sum = sum(paid_by_patient))
result$binded <- paste(result$n,result$n_pt,sep="/")
result$binded <- paste(result$binded,result$cost_visit_sum,sep="/")
result$binded <- paste(result$binded,result$cost_payer_sum,sep="/")
result$binded <- paste(result$binded,result$cost_patient_sum,sep="/")
result <- result %>% select(-n,-n_pt,-cost_visit_sum,-cost_payer_sum,-cost_patient_sum)

result$oriental <- ifelse(result$oriental==0,"Oriental","Medicine")
result <- result  %>% group_by(period,dx_location,gender,age_cat,region,diab_final,corona,oriental,total_type) %>% 
  spread(oriental,binded)
result <- result %>% separate(Oriental, into=c("n_ori","n_ori_pt","ori_visit_sum","ori_payer_sum","ori_patient_sum"))
result <- result %>% separate(Medicine, into=c("n_med","n_med_pt","med_visit_sum","med_payer_sum","med_patient_sum"))
result[is.na(result)] <- 0 
result$n_ori <- as.numeric(result$n_ori)
result$n_ori_pt <- as.numeric(result$n_ori_pt)
result$ori_visit_sum <- as.numeric(result$ori_visit_sum)
result$ori_payer_sum <- as.numeric(result$ori_payer_sum)
result$ori_patient_sum <- as.numeric(result$ori_patient_sum)

result$n_med <- as.numeric(result$n_med)
result$n_med_pt <- as.numeric(result$n_med_pt)
result$med_visit_sum <- as.numeric(result$med_visit_sum)
result$med_payer_sum <- as.numeric(result$med_payer_sum)
result$med_patient_sum <- as.numeric(result$med_patient_sum)

# result$ori_visit_mean <- as.numeric(result$ori_visit_mean)
# result$ori_payer_mean <- as.numeric(result$ori_payer_mean)
# result$ori_patient_mean <- as.numeric(result$ori_patient_mean)
# 
# result$n_med <- as.numeric(result$n_med)
# result$n_med_pt <- as.numeric(result$n_med_pt)
# result$med_visit_mean <- as.numeric(result$med_visit_mean)
# result$med_payer_mean <- as.numeric(result$med_payer_mean)
# result$med_patient_mean <- as.numeric(result$med_patient_mean)


result$period <- as.factor(result$period)
result$dx_location <- as.factor(result$dx_location)
result$gender <- as.factor(result$gender)
result$age_cat <- as.factor(result$age_cat)
result$region <- as.factor(result$region)
result$diab_final <- as.factor(result$diab_final)
result$corona <- as.factor(result$corona)



write_csv(result,"./SS/SS_statistics_visit.csv")


# result <- pt_visit %>% group_by(period,dx_location,gender,age_cat,region,diab_final,corona,oriental,total_type) %>%
#   dplyr::summarise(
#     n=n(),
#     n_pt = length(unique(person_id)))
# result$binded <- paste(result$n,result$n_pt,sep="/")
# result <- result %>% select(-n,-n_pt)
# result
# result$oriental <- ifelse(result$oriental==1,"Oriental","Medicine")
# result <- result  %>% group_by(dx_location,gender,age_cat,region,diab_final,corona,oriental,total_type) %>%
#   spread(oriental,binded)
# result <- result %>% separate(Oriental, into=c("n_ori","n_ori_pt"))
# result <- result %>% separate(Medicine, into=c("n_med","n_med_pt"))
# result[is.na(result)] <- 0
# result$n_med <- as.numeric(result$n_med)
# result$n_med_pt <- as.numeric(result$n_med_pt)
# 
# 
# 
# result$period <- as.factor(result$period)
# result$dx_location <- as.factor(result$dx_location)
# result$gender <- as.factor(result$gender)
# result$age_cat <- as.factor(result$age_cat)
# result$region <- as.factor(result$region)
# result$diab_final <- as.factor(result$diab_final)
# result$corona <- as.factor(result$corona)
# summary(result)
# write_csv(result,"./SS/SS_statistics_visit.csv")



#data <- read_excel("~/Documents/SNUH CDM/별첨3. 심평원_빅데이터실_CDM_임상테이블15종_데이터샘플.xlsx",sheet = "VISIT_OCCURRENCE")
#data %>% dplyr::summarise(n=n(),avg=mean(TOTAL_PAID))
#names(data)
# visit_occurrence에 대한 행 10518만 있으면 결측값이 존재할 수 없음.


#result <- pt_visit %>% group_by(dx_location,gender,age_cat,region,diab_pre,corona,oriental) %>% dplyr::summarise(
#                                                                                                  n=n(),
#                                                                                                  cost_visit_sum=sum(na.omit(visit_total_paid)),
#                                                                                                  cost_visit_mean=mean(na.omit(visit_total_paid)),
#                                                                                                  cost_payer_mean = mean(na.omit(paid_by_payer)),
#                                                                                                  cost_patient_mean = mean(na.omit(paid_by_patient)),
#                                                                                                  cost_drug_sum = sum(na.omit(drug_total_paid)),
#                                                                                                  cost_drug_mean = mean(na.omit(drug_total_paid)))
#result$visit_count <- result %>% dplyr::summarise(count = cost_visit_sum/cost_visit_mean)
#result$visit_count <- result %>% dplyr::summarise(count = cost_drug_sum/cost_drug_mean)
#result <- result %>% select(-cost_visit_sum, -cost_drug_sum)

######################################################################################################################
# 그룹별 평균 약제 비용
######################################################################################################################
names(pt_drug)
pt_drug %>% nrow()
#colSums(is.na(pt_pre2))
pt_drug %>% head(50)
names(pt_drug)[names(pt_drug)=="gender_source_value"] <- "gender"
names(pt_drug)[names(pt_drug)=="diab_final"] <- "diabetes"
pt_drug <- na.omit(pt_drug)
names(pt_drug)
# 실험용 
#pt_visit$visit_total_paid <- sample(x=1:30,size=nrow(pt_visit),replace=T)  

######################################Baseline Characteristics###############################
myVars = c("gender","age_cat","dx_location","diabetes","corona","region","period","drug_type")
catVars = c("gender","age_cat","dx_location","diabetes","corona","region","period","drug_type")
t2 <- CreateTableOne(data=pt_drug,vars = myVars, factorVars = catVars,strata=c("period"))
table2 <- print(t2,showAllLevels=T)
write.csv(table2, file = "./SS/ss_baseline_characteristics_drug.csv")


non <- pt_drug %>% filter(drug_type == "non_drug") %>% select(period,drug_total_paid)
dr <- pt_drug %>% filter(drug_type == "drug") %>% select(period,drug_total_paid)
neu <- pt_drug %>% filter(drug_type == "neu") %>% select(period,drug_total_paid)

sink("./SS/SS_drug_ttest.txt")
print("PRE와 POST 두 시기의 전체 평균 비교")
var.test(pt_drug$drug_total_paid[pt_drug$period=="PRE"],pt_drug$drug_total_paid[pt_drug$period=="POST"])
t.test(pt_drug$drug_total_paid[pt_drug$period=="PRE"],pt_drug$drug_total_paid[pt_drug$period=="POST"],var.equal = T)
t.test(pt_drug$drug_total_paid[pt_drug$period=="PRE"],pt_drug$drug_total_paid[pt_drug$period=="POST"],var.equal = F)

print("PRE와 POST 두 시기의 비마약성 약물 비용 평균 비교")
var.test(non$drug_total_paid[non$period=="PRE"],non$drug_total_paid[non$period=="POST"])
t.test(non$drug_total_paid[pt_drug$period=="PRE"],non$drug_total_paid[non$period=="POST"],var.equal = T)
t.test(non$drug_total_paid[pt_drug$period=="PRE"],non$drug_total_paid[non$period=="POST"],var.equal = F)

print("PRE와 POST 두 시기의 마약성 약물 비용 평균 비교")
var.test(dr$drug_total_paid[dr$period=="PRE"],dr$drug_total_paid[dr$period=="POST"])
t.test(dr$drug_total_paid[dr$period=="PRE"],dr$drug_total_paid[dr$period=="POST"],var.equal = T)
t.test(dr$drug_total_paid[dr$period=="PRE"],dr$drug_total_paid[dr$period=="POST"],var.equal = F)

print("PRE와 POST 두 시기의 신경병성 약물 비용 평균 비교")
var.test(neu$drug_total_paid[neu$period=="PRE"],neu$drug_total_paid[neu$period=="POST"])
t.test(neu$drug_total_paid[neu$period=="PRE"],neu$drug_total_paid[neu$period=="POST"],var.equal = T)
t.test(neu$drug_total_paid[neu$period=="PRE"],neu$drug_total_paid[neu$period=="POST"],var.equal = F)

sink()


non_drug <- pt_drug %>% filter(drug_type == "non_drug") %>% 
  group_by(period,dx_location,gender,age_cat,region,diab_final,corona,oriental,total_type) %>% 
  dplyr::summarise(n_nd=n(),
            nd_pt = length(unique(person_id)),
            cost_nd_sum = sum(drug_total_paid))
result_drug <- non_drug


drug <- pt_drug %>% filter(drug_type == "drug") %>% 
  group_by(period,dx_location,gender,age_cat,region,diab_final,corona,oriental,total_type) %>% 
  dplyr::summarise(n_d=n(),
            d_pt = length(unique(person_id)),
            cost_d_sum = sum(drug_total_paid))
result_drug <- result_drug %>% full_join(drug, by= c('period','dx_location','gender','age_cat','region','diab_final','corona','oriental','total_type'))

neuro <- pt_drug %>% filter(drug_type == "neuropathic pain") %>% 
  group_by(period,dx_location,gender,age_cat,region,diab_final,corona,oriental,total_type) %>% 
  dplyr::summarise(n_neuro=n(),
            neuro_pt = length(unique(person_id)),
            cost_neuro_sum = sum(drug_total_paid))


result_drug <- result_drug %>% full_join(neuro, by= c('period','dx_location','gender','age_cat','region','diab_final','corona','oriental','total_type'))
colSums(is.na(result_drug))
result_drug[is.na(result_drug)] <- 0


result_drug$period <- as.factor(resul_drugt$period)
result_drug$dx_location <- as.factor(result_drug$dx_location)
result_drug$gender <- as.factor(result_drug$gender)
result_drug$age_cat <- as.factor(result_drug$age_cat)
result_drug$region <- as.factor(result_drug$region)
result_drug$diab_final <- as.factor(result_drug$diab_final)
result_drug$corona <- as.factor(result_drug$corona)
write_csv(result_drug,"./SS/SS_statistics_drug.csv")


file.remove("./SS/SS_first_condition_ID.csv")
file.remove("./SS/SS_pre_patient_ID.csv")
file.remove("./SS/SS_post_patient_ID.csv")
file.remove("./SS/SS_pre_pt1.csv")
file.remove("./SS/SS_pre_pt2.csv")
file.remove("./SS/SS_post_pt1.csv")
file.remove("./SS/SS_post_pt2.csv")
file.remove("./SS/SS_total_pre.csv")
file.remove("./SS/SS_total_pre2.csv")
file.remove("./SS/SS_total_post.csv")
file.remove("./SS/SS_total_post2.csv")
file.remove("./SS/SS_total_visit.csv")

# non_drug <- pt_pre2 %>% filter(drug_type == "non_drug") %>% 
#   group_by(dx_location,gender,age_cat,region,diab_final,corona,oriental,total_type) %>% 
#   dplyr::summarise(n_nd=n(),
#             nd_pt = length(unique(person_id)))

# drug <- pt_pre2 %>% filter(drug_type == "drug") %>% 
#   group_by(dx_location,gender,age_cat,region,diab_final,corona,oriental,total_type) %>% 
#   dplyr::summarise(n_d=n(),
#             d_pt = length(unique(person_id)))
# neuro <- pt_pre2 %>% filter(drug_type == "neuropathic pain") %>% 
#   group_by(dx_location,gender,age_cat,region,diab_final,corona,oriental,total_type) %>% 
#   dplyr::summarise(n_neuro=n(),
#             neuro_pt = length(unique(person_id)))
