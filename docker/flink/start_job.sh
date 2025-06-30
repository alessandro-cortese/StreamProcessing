#!/bin/bash

for i in $(seq 1 8); do
  mkdir -p ../Results/task_manager_number_${i}
  mkdir -p ../Results/task_manager_number_${i}_opt
done


flink run -c it.uniroma2.sabd.MainJob /opt/flink/usrlib/thermal-defect-1.0.jar