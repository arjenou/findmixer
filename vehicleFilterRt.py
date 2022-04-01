import cv2
import glob
import os
from yolov4.tf import YOLOv4
import numpy as np
import time as ti
import json
import utilityRt as ut
from utilityRt import time_queue, datebase, dearea, line1, line2, filedate, camerawidth, cameraheight
import math

def mixer():
    yolo = YOLOv4()
    yolo.config.parse_names("./yolovtest/truck.names")
    yolo.config.parse_cfg("./yolovtest/yolo-obj-Copy1.cfg")
    yolo.make_model()
    yolo.load_weights("./yolovtest/yolov-obj_0324.weights", weights_type="yolo")
    yolo.summary(summary_type="yolo")
    yolo.summary()
    area_def_file = 'camareas.json'
#---------------------------------------------------------yolov4_mixer----------------------------------------------------------------------         
    while True:
        if time_queue.empty():
            ti.sleep(1)
            continue           
        savename=time_queue.get()
        picturename='./data/'+filedate+'/log_'+savename+'.jpg'
        jsonname='./data/'+filedate+'/log_'+savename+'.json'      
        image = cv2.imread(picturename)
        bboxes = yolo.predict(frame=image,prob_thresh=0.25)
        print(bboxes)
        debug_image = yolo.draw_bboxes(image, bboxes)
        cv2.imwrite(f"./done/detectedTruck/log_{savename}.jpg",debug_image)
        if bboxes[0][0]==bboxes[0][2]==bboxes[0][3]==bboxes[0][4]==bboxes[0][5]==0:
            continue
#__________________________________________areason____________________________________
        json_open = open(area_def_file, 'r')       
        areadef = json.load(json_open)
        polygon = []
        for areaidx, area in enumerate(areadef['areas']):
            tmp=[]
            for point in area['points']:
                tmp.append([point['x'],point['y']])
            polygon.append(np.array(tmp))
        if len(polygon) > 4:
            print("More than 2 areas are found. Only the first two will be used.")
#_____________________________________________which car is the nearest____________________________________
        shortest_distance = 1000000
        selectbboxes = 0
        direction,lable_name,x,y,w,h = dearea(jsonname)
        midX, midY = (x+(w/2), y+(h/2))
        for i, car in enumerate(bboxes):
            #print(cropx,cropy)
            bboxesX, bboxesY = (car[0]*camerawidth, car[1]*cameraheight)
            tmp_shortest_distance = math.sqrt((midX - bboxesX)**2+(midY - bboxesY)**2)
            if tmp_shortest_distance < shortest_distance:
                shortest_distance = tmp_shortest_distance
                selectbboxes = i
        car=bboxes[selectbboxes]
        datebase(jsonname,savename,car[4],0)
        debug_image = yolo.draw_bboxes(image, bboxes)
        cv2.imwrite(f"./done/passedLineTruck/log_{savename}.jpg",debug_image)
        bboxesXY = (car[0]*camerawidth, car[1]*cameraheight)
        #print(bboxesX,bboxesY)
#------------------------------------------------------------------AreaChecking---------------------------------------------------------
        areaChecked = 0
        if lable_name == "Line 1":   
            if direction == line1['INdirection']:
                areaChecked = line1['INarea']
            else:
                areaChecked = line1['OUTarea']
        else:
            if direction == line2['INdirection']:
                areaChecked = line2['INarea']
            else:
                areaChecked = line2['OUTarea']

        if cv2.pointPolygonTest(polygon[areaChecked], bboxesXY, False) > 0:
            debug_image = yolo.draw_bboxes(image, bboxes)       
            if car[4] == 1:
                cv2.imwrite(f"./done/passedAreaMixer/log_{savename}.jpg",debug_image)
                datebase(jsonname,savename,car[4],1)
                #with open(f'./done/mixer/{recoverdate}_mixer.txt', 'a') as f:
                    #f.writelines(f"{jsonname}\n")
            else:
                cv2.imwrite(f"./done/passedAreaOthers/log_{savename}.jpg",debug_image)
                datebase(jsonname,savename,car[4],2)
