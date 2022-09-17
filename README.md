# monad_playground

Experiment with some application of Monad design pattern


# TODO:

## RayMonad 
- [X] Creat a monad that bind to ray.serve DAG.
    - [X] Start with a little playground of ray.serve DAG. (here: https://github.com/jeffrey82221/ray_playground) 
    - [X] Come up with a connection scheme of monad to the DAG.
    - [X] Create a basic monad and the nested monad class.

## Lightflow Monad 
- [ ] Create a monad that bind to lightflow for Single Machine ETL speed up and management. 
    - [ ] Start with a lightflow playground 
        - [ ] Setup up the redis / mongoDB boker
        - [ ] Successfully run an example 
    - [ ] Come up with a connection scheme of monad to the DAG.
    - [ ] Create a basic monad and the nested monad class. 

## ParaDag Monad 
- [ ] Create a monad that leverage the simple Multi-threading execution mode of paradag to speed up IO bounded workflow. 
    - [ ] Start with a paradag playground 
        - [ ] experiment with the topology sorting machenism 
        - [ ] experiment with the multi-thread execution mechanism 

    - [ ] Come up with a connection scheme of monad to the DAG. 
    - [ ] Create a basic monad and the nested monad class. 

## TorchMonad
- [ ] Using RayMonad framework to decompose a large pytorch model into multiple smaller models to speed up the API inference time (plus also convert the small models into torchscript to enhance their runing time)
    - [ ] Find a complicated pre-trained model. (Ref: https://towardsdatascience.com/ensembles-the-almost-free-lunch-in-machine-learning-91af7ebe5090#bcd1)
    - [ ] Naively decompose the model into smaller parts and make sure 
        they can be connected and operated correctly. 
    - [ ] Try to make torch script of the smaller part and make sure they can be connected together 
    - [ ] Profiling the speed up benefit of torch script 
    - [ ] Adapt the decomposition and torch script transformation to RayMonad 
    - [ ] Make speed up profiling on the new monad. 

## TorchBatwiseMonad
- [ ] Using LightflowMonad framework to decompose a large pytorch model into multiple smaller models that speed up Batchwise inference time 
    - Goal 1: We can selectively increase the worker count of certain part of the model (Meanwhile, selectively determine the Batchsize of each worker)
    - Goal 2: We can parallelly run the different stages of the model to increase throughput (REF: https://www.allaboutcircuits.com/uploads/articles/Pipeline_4_stage_2_.jpg)    

## Profiling:
- [ ] 加入碳足跡、memory、時間的profiling: https://github.com/udothemath/ml_with_graph_algorithms/blob/9ac1e8ae00ae64bb5f0b815c234f56712cd1f87a/etl_study/etl_study/profile_framework/utils/profile.py#L46
- [ ] 研究codecarbon tracking_mode='machine'，是否只檢測單一個container的狀態
