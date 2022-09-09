# monad_playground

Experiment with some application of Monad design pattern

# TODO:
- [ ] 加入碳足跡、memory、時間的profiling: https://github.com/udothemath/ml_with_graph_algorithms/blob/9ac1e8ae00ae64bb5f0b815c234f56712cd1f87a/etl_study/etl_study/profile_framework/utils/profile.py#L46
- [ ] 研究codecarbon tracking_mode='machine'，是否只檢測單一個container的狀態

# TODO:
- [ ] creat a monad that bind to ray.serve DAG.
    - [ ] Start with a little playground of ray.serve DAG. (here: https://github.com/jeffrey82221/ray_playground) 
    - [ ] Come up with a connection scheme of monad to the DAG.
    - [ ] Create a basic monad and the nested monad class.
