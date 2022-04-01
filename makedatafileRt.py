# _*_coding:utf-8_*_
import os
import time as ti
p=r'/sdk/projects/project1/notebooks/mixer_cam/data/'
def create_folder():
    day = ti.strftime("%Y-%m-%d", ti.localtime()) 
    foldername = p+day
    word_name = os.path.exists(foldername)
    if not word_name:
        os.makedirs(foldername)
    