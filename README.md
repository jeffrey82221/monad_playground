# monad_playground
Experiment with some application of Monad design pattern  


# TODO:
- 加入碳足跡、memory、時間的profiling: https://github.com/udothemath/ml_with_graph_algorithms/blob/9ac1e8ae00ae64bb5f0b815c234f56712cd1f87a/etl_study/etl_study/profile_framework/utils/profile.py#L46
- 研究codecarbon tracking_mode='machine'，是否只檢測單一個container的狀態


# NOTE: Resolve conflict 

## resolve conflict on minio tmpfile 

- [ ] Make sure tmpfile name of minio is unique for repeated function binded on different stage
- [ ] Make sure tmpfile name of minio is unique for repeated function binded on different instance 
    (Same `Group` child class may have different instance. An instance id is required as identifier) 
- [ ] Identifiers: `module_id`, `class_id`, `instance_id`, `bind_id`, `in or out`

## resolve conflict on db table name 

- [ ] Make sure all table names are unique 
- [ ] No altering the table names in yaml 
- [ ] If the same `Group` class is repeatively initialized, there should have different yaml for each instance.
- [ ] The name of unique table_name can be assigned via f-string when the Group instance is initialized. 