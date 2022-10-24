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
library('DatabaseConnector')

# Hospital information
hname="SNUH"

# working directory
# mywd="" # plaese change working directory

# DB 변수 입력
dbms <- ""
user <- ""
password <- ""
server <- ""
port <-
pathToDriver <- ""

myschemaname=''    # OMOP CDM schema name
myvocabschemaname='' # OMOP CDM vocabulary schema name
mytmpschemaname=''
oracleTempSchema <- NULL

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = password,
                                                                port = port,
                                                                pathToDriver = pathToDriver)
con <- DatabaseConnector::connect(connectionDetails)




# set work directory
wd <- getwd() 


if (str_sub(wd,-17,-1)=="SNUH_SPINE_result"){print(wd)} else {
  result_path <- paste0(wd,"/SNUH_SPINE_result")
  if (dir.exists(result_path)==FALSE){dir.create(result_path)}
  setwd(result_path)
}



coh <- read_csv("./SPL/spl_total_visit.csv") #75813
coh <- coh %>% select(-condition_start_date,-condition_occurrence_id)
coh <- unique(coh) #중복행제거
coh %>% nrow() # 8157
#coh %>% group_by(visit_occurrence_id) %>% dplyr::summarise(n=n()) %>% filter(n>2)
#coh %>% filter(visit_occurrence_id == 6129633)
names(coh)
## 전체 환자에 대해서 covariate 생성해주고, 각 연도별로 추출할 것
# covariate : 성별,나이(카테고리),척추 location, 지역,당뇨병 약제 사용 여부,병원 type,코로나 진단 여부  
#########################################PRE###################################################################
patient_pre <- coh %>% filter(visit_start_date>='2018-01-01' & visit_start_date <'2020-03-01') # %>% select(-visit_start_date,-visit_end_date)
patient_pre <- unique(patient_pre)

sink("./SPL/SPL_PRE_PATIENT_NUM.txt",split=TRUE)
print("PRE")
patient_pre$person_id %>% unique() %>% length() #1730명
patient_pre %>% nrow() #6205
sink()
#patient_pre <- patient %>% filter(condition_start_date>='2018-01-01' & condition_start_date <'2020-03-01') 
names(patient_pre)
##################################################################################################################
# 나이 카테고리, 성별, 척추 LOCATION 추가
#####################################################################################################################
patient_pre$age_cat <- case_when(patient_pre$age <50 ~ "40s",
                                 patient_pre$age <60 ~ "50s",
                                 patient_pre$age <70 ~ "60s",
                                 patient_pre$age <80 ~ "70s",
                                 patient_pre$age >=80 ~ "80s up")
patient_pre %>% nrow()
patient_pre$'Dx_location' <-case_when(patient_pre$condition_concept_id %in% c(4167097, 37088427, 42492165, 42617470, 45548416, 37088436, 42492174, 42617479, 45582144) ~ "SPL_X",
                                      patient_pre$condition_concept_id %in% c(36713107, 36713108, 4145719, 37088435, 42492173, 42617478, 45596576) ~ "SPL_L",
                                      patient_pre$condition_concept_id %in% c(80497, 37088429, 42492167, 42617472, 45591834, 37088430, 42492168, 42617473, 45562721) ~ "SPL_C")                          
#patient$'Dx_location' <- as.factor(patient$'Dx_location')  # 서울대병원 협착증 코드 : 세종류 다 있음
dim(patient_pre)[1] #6205

#patient_pre %>% filter(Dx_location=="spl_X") %>% nrow() # 42331

##### spl_X 환자에 대해서 SS_L, SS_C로 변환하기 ########################################################################
## 원 데이터에서 spl_X와 spl_L or spl_C 두개이상의 코드를 가지는 경우 -> spl_X 일괄 삭제해주기
more_loc <- patient_pre %>% group_by(person_id,visit_occurrence_id) %>% dplyr::summarise(n=length(unique(Dx_location))) %>% filter(n==2) # Dx_location이 하나만 존재하거나, 세개 다 존재하는 경우는 제외 
more_loc <- patient_pre %>% filter((person_id %in% more_loc$person_id) & (visit_occurrence_id %in% more_loc$visit_occurrence_id)) %>% group_by(person_id, visit_occurrence_id, Dx_location) %>% dplyr::summarise(n=n()) %>% select(-n)
more_loc_x <- more_loc %>% filter(Dx_location == "SPL_X") %>% group_by(person_id,visit_occurrence_id,Dx_location)
more_loc_x %>% nrow() #433
more_loc_l_c <- more_loc %>% filter(Dx_location != "SPL_X") %>% group_by(person_id,visit_occurrence_id,Dx_location)
patient_pre <- patient_pre %>% filter(!((person_id %in% more_loc_x$person_id)&(visit_occurrence_id %in% more_loc_x$visit_occurrence_id)&(Dx_location == "SPL_X")))
# 44213
more_loc_x %>% group_by(person_id,visit_occurrence_id) %>% dplyr::summarise(n=length(unique(Dx_location))) %>% filter(n != 1) ## 한사람의 한 방문당 SPL_X 코드는 한번씩만 나와야함 => 0인게 맞음
patient_pre %>% filter(((person_id %in% more_loc_x$person_id)&(visit_occurrence_id %in% more_loc_x$visit_occurrence_id)&(Dx_location == "SPL_X"))) %>% group_by(person_id,visit_occurrence_id,Dx_location) %>% dplyr::summarise(n=n()) %>% filter(n!=1) # 얘도 0인게 맞음
#patient_pre %>% filter(((person_id %in% more_loc_x$person_id)&(visit_occurrence_id %in% more_loc_x$visit_occurrence_id)&(Dx_location == "SPL_X"))) %>% group_by(person_id,visit_occurrence_id,Dx_location) %>% dplyr::summarise(n=n()) %>% filter((person_id==72466)&(visit_occurrence_id==6576303))


## 1차 수정 후에도 남아있는 SPL_X -> X_RAY 결과를 가지고 2차 수정
spl_x <- patient_pre %>% filter(Dx_location == "SPL_X")
spl_x %>% nrow() # 41898
dbWriteTable(con, 'spl_x', value=spl_x, row.names=FALSE, overwrite=TRUE)
spl_x<- dbReadTable(con, "spl_x")

#xray <-executeSql(con,"DROP TABLE IF EXISTS xray;")
xray <- executeSql(con,"BEGIN
                         EXECUTE IMMEDIATE 'DROP TABLE xray';
                         EXCEPTION WHEN OTHERS THEN NULL;
                        END;")
xray <- translate("CREATE TEMP TABLE xray AS(SELECT DISTINCT procedure_occurrence_id,person_id,procedure_concept_id, procedure_date, visit_occurrence_id FROM @A.procedure_occurrence
                  WHERE visit_occurrence_id IN (SELECT DISTINCT visit_occurrence_id FROM spl_x)
                  AND procedure_concept_id IN (4281665,4070901,4236024,4263302)
                  AND person_id IN (SELECT DISTINCT person_id FROM spl_x));",targetDialect=connectionDetails$dbms,
             oracleTempSchema=oracleTempSchema)
xray <-render(xray,A=myschemaname)
xray <- executeSql(con,xray)
xray <- querySql(con,"SELECT * FROM xray;")

dbWriteTable(con, 'xray', value=xray, row.names=FALSE, overwrite=TRUE)
xray<- dbReadTable(con, "xray")
colnames(xray) <- tolower(colnames(xray))
xray %>% nrow() # 260

length(unique(xray$person_id)) #x-ray 기록이 있는 환자가 58명 

all(unique(xray$person_id) %in% unique(spl_x$person_id))
x_ray <- spl_x %>% left_join(xray,by=c('person_id','visit_occurrence_id'))
x_ray %>% nrow() #41925
x_ray_count <- x_ray %>% filter(!is.na(procedure_concept_id))
x_ray_count %>% nrow() # x-ray 기록 자체는 319개 
colnames(x_ray_count) <- tolower(colnames(x_ray_count))
x_ray_count$new_code <- case_when(x_ray_count$procedure_concept_id %in% c(4281665,4070901) ~ "SPL_C",
                                  x_ray_count$procedure_concept_id %in% c(4236024,4263302) ~ "SPL_L")
#x_ray$procedure_concept_id[is.na(x_ray$procedure_concept_id)==T] = "N"
#x_ray_count$new_code %>% unique() # SPL_C만 있넴
x_ray_count <- x_ray_count %>% group_by(person_id,visit_occurrence_id,new_code) %>% dplyr::summarise(pro_count = n()) # 환자별로 경추,요추 x-ray를 각각 몇번 찍었는지 확인
x_ray_count <- x_ray_count %>% spread(new_code,pro_count)
x_ray_count$SPL_C[is.na(x_ray_count$SPL_C)] <- 0
x_ray_count$SPL_L[is.na(x_ray_count$SPL_L)] <- 0
x_ray_count <- x_ray_count %>% filter(!((SPL_C %in% c(0,1))&(SPL_L %in% c(0,1))))
x_ray_count$final <- case_when(x_ray_count$SPL_C > x_ray_count$SPL_L ~ "SPL_C",
                               x_ray_count$SPL_C < x_ray_count$SPL_L ~ "SPL_L",
                               x_ray_count$SPL_C == x_ray_count$SPL_L ~ "SPL_X")


spl_c <- x_ray_count %>% filter(final == "SPL_C")
spl_l <- x_ray_count %>% filter(final == "SPL_L")
patient_pre[(patient_pre$person_id %in% spl_c$person_id)&(patient_pre$visit_occurrence_id %in% spl_c$visit_occurrence_id),]$Dx_location = "SPL_C" 
patient_pre[(patient_pre$person_id %in% spl_l$person_id)&(patient_pre$visit_occurrence_id %in% spl_l$visit_occurrence_id),]$Dx_location = "SPL_L"

dbWriteTable(con, 'patient_pre', value=patient_pre, row.names=FALSE, overwrite=TRUE)
patient_pre <- dbReadTable(con, "patient_pre") # 전방전위증군에 해당하는 모든 visit_occurrence

sink("./SPL/SPL_PRE_PATIENT_NUM.txt",append=TRUE,split=TRUE)
print("Dx_location")
patient_pre$person_id %>% unique() %>% length() #1730명
patient_pre %>% nrow() #6205
sink()
names(patient_pre)
##################################################################################################################
# 당뇨병 약제 사용 여부
#####################################################################################################################
# 당뇨부터 다시 
#diabetes <-executeSql(con,"DROP TABLE IF EXISTS diabetes;")
diabetes <- executeSql(con,"BEGIN
                             EXECUTE IMMEDIATE 'DROP TABLE diabetes';
                             EXCEPTION WHEN OTHERS THEN NULL;
                            END;")
diabetes <- translate("CREATE TEMP TABLE diabetes AS (SELECT DISTINCT drug_exposure_id,person_id, drug_concept_id, drug_exposure_start_date, visit_occurrence_id FROM @A.drug_exposure
                     WHERE person_id IN (SELECT DISTINCT person_id FROM patient_pre)
                     AND drug_concept_id IN (40220376, 40220375, 40220371, 40164946, 40164939, 40164929, 40164897, 21600765, 21600747, 19106521,
                     21600768, 21600767, 21600770, 21600767, 21600761, 1597773, 1597772, 1597761, 1597758, 21600758, 19059797, 21600756, 21600750, 19077682,
                     21600776, 19021312, 1529352, 42961189, 42961179, 21600778,
                     21600790, 21600791, 19107110, 1502829, 21600796,
                     21600782, 19079293, 1525221, 21600770, 42960773,
                     5893491, 43534749, 43267262, 42961494, 42961490, 42961487, 42708176, 42708172, 42708168, 40164922, 40164891, 36887702, 21600783, 21600784, 21600772, 21169719, 21081251, 19125045, 19125049, 19125041, 21600785, 19129179, 40166041, 21600786, 40239218, 42961500, 715821, 43013924, 21600787, 42960599, 42960653, 21600783,
                     44785831, 1123890, 45774754, 1123740, 715760, 715887,
                     1123633, 42922767, 42922959,
                     46234047, 46233969, 43862455, 43534747, 43275300, 42922214, 42921679, 42921631, 42920573, 41370419, 35782557, 35782268, 35602720, 21600739, 21600733, 21600732, 21600729, 21600726, 21600723, 21600719, 21600718, 21600715, 19135264, 2028902, 2028850, 2028839, 2028813, 2027324, 2027320, 2027300, 2027289)
                     AND drug_exposure_start_date BETWEEN TO_DATE('2018-01-01','YYYY-MM-DD') AND TO_DATE('2020-02-29','YYYY-MM-DD'));",targetDialect=connectionDetails$dbms,
             oracleTempSchema=oracleTempSchema)
diabetes <-render(diabetes,A=myschemaname)
diabetes <- executeSql(con,diabetes)
diabetes <- querySql(con,"SELECT * FROM diabetes")
dbWriteTable(con, 'diabetes', value=diabetes, row.names=FALSE, overwrite=TRUE)
diabetes<- dbReadTable(con, "diabetes")
colnames(diabetes) <- tolower(colnames(diabetes))

unique(patient_pre$visit_occurrence_id %in% diabetes$visit_occurrence_id) # True, False 둘 다 있을 수 있음


patient_pre$diab_final <- ifelse(patient_pre$person_id %in% diabetes$person_id,1,0)

#unique(unique(diabetes$person_id)%in%(unique(patient_pre1$person_id))) # 뽑은 당뇨환자가 원래 patient와 매칭되는지 확인
unique(diabetes$person_id) %>% length() # 128명 # 23
#diabetes$diab_pre <- case_when(diabetes$drug_exposure_start_date>='2018-01-01' & diabetes$drug_exposure_start_date<'2020-03-01' ~ 1,
#                               diabetes$drug_exposure_end_date>='2018-01-01' & diabetes$drug_exposure_end_date<'2020-03-01' ~ 1,
#                               is.na(diabetes$drug_exposure_start_date) ~ 0,
#                               TRUE ~ 0)



#diabetes$diab_post <- case_when(diabetes$drug_exposure_start_date>='2020-03-01' & diabetes$drug_exposure_start_date<'2021-12-31' ~ 1,
#                                diabetes$drug_exposure_end_date>='2020-03-01' & diabetes$drug_exposure_end_date<'2021-12-31' ~ 1,
#                                is.na(diabetes$drug_exposure_start_date) ~ 0,
#                                TRUE ~ 0)

### 각 시기별로 0,1이 동시에 있는 환자 추출 (동시에 있으면 1로 배정, 둘 중 하나만 있으면 0 or 1)
#pre_patient <- patient_pre1 %>% group_by(person_id,diab_pre) %>% dplyr::summarise(n=n())
#pre_patient <- pre_patient %>% group_by(person_id) %>% dplyr::summarise(cat_num=n()) 
#patient_pre <- patient_pre %>% left_join(pre_patient,by="person_id")
#patient_pre$diab_final <- case_when(patient_pre$cat_num == 2 ~1,
#                                    (patient_pre$cat_num == 1)&(patient_pre$diab_pre==1)~1,
#                                    TRUE ~ 0)
patient_pre %>% nrow()
colSums(is.na(patient_pre))
#patient_pre <- patient_pre %>% select(-cat_num,-diab_pre)
patient_pre <- unique(patient_pre)
patient_pre %>% nrow() # 44213

sink("./SPL/SPL_PRE_PATIENT_NUM.txt",append=TRUE,split=TRUE)
print("당뇨여부")
patient_pre$person_id %>% unique() %>% length() #1730명
patient_pre %>% nrow() #6205
sink()

##################################################################################################################
# 코로나 진단 여부
#####################################################################################################################
#corona <-executeSql(con,"DROP TABLE IF EXISTS corona;")
corona <- executeSql(con,"BEGIN
                           EXECUTE IMMEDIATE 'DROP TABLE corona';
                           EXCEPTION WHEN OTHERS THEN NULL;
                          END;")
corona <- translate("CREATE TEMP TABLE corona AS(SELECT DISTINCT person_id, condition_start_date as corona_date FROM @A.condition_occurrence
                    WHERE person_id IN (SELECT DISTINCT person_id FROM patient_pre)
                    AND condition_concept_id IN (37311061,439676,4100065));",targetDialect=connectionDetails$dbms,
             oracleTempSchema=oracleTempSchema)
corona <-render(corona,A=myschemaname)
corona <- executeSql(con,corona)
corona <- querySql(con,"SELECT * FROM corona;")
dbWriteTable(con, 'corona', value=corona, row.names=FALSE, overwrite=TRUE)
corona <- dbReadTable(con, "corona")
colnames(corona) <- tolower(colnames(corona))

#patient <- patient %>% left_join(corona, by= "person_id")
patient_pre$corona <- ifelse(patient_pre$person_id %in% corona$person_id,1,0) #코로나 확진 됐으면 1, 아니면 0
dbWriteTable(con, 'patient_pre', value=patient_pre, row.names=FALSE, overwrite=TRUE)
patient_pre <- dbReadTable(con, "patient_pre") # 전방전위증군에 해당하는 모든 visit_occurrence
patient_pre %>% nrow() #6205

sink("./SPL/SPL_PRE_PATIENT_NUM.txt",append=TRUE,split=TRUE)
print("코로나 여부")
patient_pre$person_id %>% unique() %>% length() #1730명
patient_pre %>% nrow() #6205
sink()
##################################################################################################################
# 병원 위치
#####################################################################################################################
#location <-executeSql(con,"DROP TABLE IF EXISTS location;")
location <- executeSql(con,"BEGIN
                             EXECUTE IMMEDIATE 'DROP TABLE location';
                             EXCEPTION WHEN OTHERS THEN NULL;
                            END;")
location <- translate("CREATE TEMP TABLE location AS (SELECT DISTINCT care_site_id, care_site_name, location_id, place_of_service_source_value FROM @A.care_site
                      WHERE care_site_id IN (SELECT DISTINCT care_site_id FROM patient_pre));",targetDialect=connectionDetails$dbms,
             oracleTempSchema=oracleTempSchema)
location <-render(location,A=myschemaname)
location <- executeSql(con,location)
location <- querySql(con,"SELECT * FROM location;")

dbWriteTable(con, 'location', value=location, row.names=FALSE, overwrite=TRUE)
location<- dbReadTable(con, "location")
colnames(location) <- tolower(colnames(location))

patient_pre <- patient_pre %>% left_join(location, by="care_site_id") #%>% nrow()
dbWriteTable(con, 'patient_pre', value=patient_pre, row.names=FALSE, overwrite=TRUE)
patient_pre <- dbReadTable(con, "patient_pre") # 전방전위증군에 해당하는 모든 visit_occurrence
patient_pre %>% nrow()
patient_pre$region <- case_when(patient_pre$location_id %in% 1:31 ~ "Busan",
                            patient_pre$location_id %in% 32:49 ~ "Daegu",
                            patient_pre$location_id %in% 50:63 ~ "Daejeon",
                            patient_pre$location_id %in% 64:75 ~ "Gwangju",
                            patient_pre$location_id %in% c(76:93,452) ~ "Incheon",
                            patient_pre$location_id %in% 94:95 ~ "Sejong-si",
                            patient_pre$location_id %in% 96:165 ~ "Seoul",
                            patient_pre$location_id %in% 166:174 ~ "Ulsan",
                            patient_pre$location_id %in% 175:196 ~ "Chungcheongbuk-do",
                            patient_pre$location_id %in% 197:220 ~ "Chungcheongnam-do",
                            patient_pre$location_id %in% 221:246 ~ "Gangwon-do",
                            patient_pre$location_id %in% 247:324 ~ "Gyeonggi-do",
                            patient_pre$location_id %in% 325:365 ~ "Gyeongsangbuk-do",
                            patient_pre$location_id %in% 366:396 ~ "Gyeongsangnam-do",
                            patient_pre$location_id %in% 397:402 ~ "Jeju-do",
                            patient_pre$location_id %in% 403:424 ~ "Jyeollabuk-do",
                            patient_pre$location_id %in% 425:451 ~ "Yeosu-si",
                            TRUE ~ "0")
dbWriteTable(con, 'patient_pre', value=patient_pre, row.names=FALSE, overwrite=TRUE)
patient_pre <- dbReadTable(con, "patient_pre") # 전방전위증군에 해당하는 모든 visit_occurrence
patient_pre %>% nrow() # 6205

sink("./SPL/SPL_PRE_PATIENT_NUM.txt",append=TRUE,split=TRUE)
print("병원 위치")
patient_pre$person_id %>% unique() %>% length() #1730명
patient_pre %>% nrow() #6205
sink()
##################################################################################################################
# 병원 type (양/한)
#####################################################################################################################
patient_pre$oriental <- ifelse(patient_pre$place_of_service_source_value %in% c(92,93),1,0) # 한의원 : 93, 한방병원 : 92 
patient_pre$oriental
hosp_type <- patient_pre %>% group_by(person_id,oriental)%>% dplyr::summarise(n=n())
hosp_type2 <- hosp_type %>% group_by(person_id) %>% dplyr::summarise(type_count=n())
hosp_type <- hosp_type %>% left_join(hosp_type2,by="person_id")
hosp_type %>% nrow()
hosp_type$total_type <-case_when(hosp_type$type_count==2~"Both",
                                 (hosp_type$type_count==1)&(hosp_type$oriental==1)~"Oriental",
                                 (hosp_type$type_count==1)&(hosp_type$oriental==0)~"Medicine"
)
hosp_type <- hosp_type %>% select(-n,-type_count)
patient_pre <- patient_pre %>% left_join(hosp_type, by=c("person_id","oriental"))
dbWriteTable(con, 'patient_pre', value=patient_pre, row.names=FALSE, overwrite=TRUE)
patient_pre <- dbReadTable(con, "patient_pre") # 전방전위증군에 해당하는 모든 visit_occurrence
patient_pre %>% nrow() # 44213
names(patient_pre)

patient_pre %>% head()

sink("./SPL/SPL_PRE_PATIENT_NUM.txt",append=TRUE,split=TRUE)
print("병원 type")
patient_pre$person_id %>% unique() %>% length() #1730명
patient_pre %>% nrow() #6205
sink()
##################################################################################################################
# 최종 데이터 저장_1
#####################################################################################################################
write_csv(patient_pre,"./SPL/spl_total_pre.csv")


patient_pre <- patient_pre %>% 
  select(-condition_concept_id,-care_site_id,-year_of_birth,-care_site_name,-place_of_service_source_value,-age,-location_id)
patient_pre %>% names()
patient_pre %>% nrow() #8446

write_csv(patient_pre, "./SPL/spl_pre_pt1.csv")

##################################################################################################################
# 약제 비용 (OUTCOME)
#####################################################################################################################
#drug <-executeSql(con,"DROP TABLE IF EXISTS drug;")
drug <- executeSql(con,"BEGIN
                         EXECUTE IMMEDIATE 'DROP TABLE drug';
                         EXCEPTION WHEN OTHERS THEN NULL;
                        END;")
drug <- translate("CREATE TEMP TABLE drug AS (SELECT DISTINCT person_id,drug_concept_id,drug_exposure_start_date,visit_occurrence_id,total_paid as drug_total_paid FROM @A.drug_exposure
                  WHERE visit_occurrence_id IN (SELECT DISTINCT visit_occurrence_id FROM patient_pre)
                  AND drug_concept_id IN (42928244, 21604344, 19096574, 19072235, 19021146, 19020053, 1127433,
                  42926622, 21603956, 19029394,
                  21603968, 19019273, 1115126,
                  21603992, 19107261, 19029025, 19029024,
                  21604298, 1103398, 792583, 1103359, 1123645, 42940040, 42939837, 42939995,
                  42922410, 40232707, 21604260, 1718702, 1718691, 792366, 21091527, 45774949, 45774943, 45774946, 21604265,
                  21604438, 19112671, 19112670, 734396,
                  21604434, 19077550, 19077549, 19077548, 19077547,
                  21604749, 19121375, 715292));",targetDialect=connectionDetails$dbms,
             oracleTempSchema=oracleTempSchema)
# drug <- translate("CREATE TEMP TABLE drug AS (SELECT DISTINCT person_id,drug_concept_id,drug_exposure_start_date,visit_occurrence_id FROM @A.drug_exposure
#                   WHERE visit_occurrence_id IN (SELECT DISTINCT visit_occurrence_id FROM patient_pre)
#                   AND drug_concept_id IN (42928244, 21604344, 19096574, 19072235, 19021146, 19020053, 1127433,
#                   42926622, 21603956, 19029394,
#                   21603968, 19019273, 1115126,
#                   21603992, 19107261, 19029025, 19029024,
#                   21604298, 1103398, 792583, 1103359, 1123645, 42940040, 42939837, 42939995,
#                   42922410, 40232707, 21604260, 1718702, 1718691, 792366, 21091527, 45774949, 45774943, 45774946, 21604265,
#                   21604438, 19112671, 19112670, 734396,
#                   21604434, 19077550, 19077549, 19077548, 19077547,
#                   21604749, 19121375, 715292));",targetDialect=connectionDetails$dbms,
#                   oracleTempSchema=oracleTempSchema)
drug <-render(drug,A=myschemaname)
drug <- executeSql(con,drug)
drug <- querySql(con,"SELECT * FROM drug;")
dbWriteTable(con, 'drug', value=drug, row.names=FALSE, overwrite=TRUE)
drug<- dbReadTable(con, "drug")
colnames(drug) <- tolower(colnames(drug))
drug %>% nrow() #4201
drug %>% head()

patient_pre <- patient_pre %>% left_join(drug,by=c("visit_occurrence_id","person_id"))
patient_pre %>% nrow() #8597
patient_pre$drug_type <- case_when(patient_pre$drug_concept_id %in% c(42928244, 21604344, 19096574, 19072235, 19021146, 19020053, 1127433,
                                                              42926622, 21603956, 19029394,
                                                              21603968, 19019273, 1115126,
                                                              21603992, 19107261, 19029025, 19029024)~"non_drug",
                               patient_pre$drug_concept_id %in% c(21604298, 1103398, 792583, 1103359, 1123645, 42940040, 42939837, 42939995,
                                                              42922410, 40232707, 21604260, 1718702, 1718691, 792366, 21091527, 45774949, 45774943, 45774946, 21604265) ~ "drug",
                               patient_pre$drug_concept_id %in% c(21604438, 19112671, 19112670, 734396,
                                                              21604434, 19077550, 19077549, 19077548, 19077547,
                                                              21604749, 19121375, 715292) ~ "neuropathic pain")
##################################################################################################################
# 최종 데이터 저장
#####################################################################################################################
write_csv(patient_pre,"./SPL/spl_total_pre2.csv")

# patient_pre$drug_concept_id[is.na(patient_pre$drug_concept_id)] <- ""
# patient_pre <- patient_pre %>% select(-drug_exposure_start_date)
# patient_pre$drug_type[is.na(patient_pre$drug_type)] <- ""
# dbWriteTable(con, 'patient_pre2', value=patient_pre, row.names=FALSE, overwrite=TRUE)
# patient_pre <- dbReadTable(con, "patient_pre2") # 전방전위증군에 해당하는 모든 visit_occurrence
# patient_pre %>% nrow() #8446 # DISTINCT 때문인듯


##################################################################################################################
# 최종 데이터 필요한 컬럼만 남기기
#####################################################################################################################
#patient_pre <- patient %>% select(person_id,first_condition,gender_source_value,age_cat,Dx_location,diab_pre,
#                   corona,diab_pre,oriental,region,drug_type,visit_total_paid,paid_by_payer,paid_by_patient,drug_total_paid)
patient_pre <- patient_pre %>% select(-drug_exposure_start_date)
patient_pre %>% nrow() #2490
write_csv(patient_pre, "./SPL/spl_pre_pt2.csv")






