# Generated by Django 4.0.2 on 2022-02-23 14:26

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Book',
            fields=[
                ('isbn', models.CharField(max_length=20, primary_key=True, serialize=False)),
                ('title', models.CharField(max_length=100)),
                ('author', models.CharField(max_length=50)),
                ('year', models.IntegerField(choices=[(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19), (20, 20), (21, 21), (22, 22), (23, 23), (24, 24), (25, 25), (26, 26), (27, 27), (28, 28), (29, 29), (30, 30), (31, 31), (32, 32), (33, 33), (34, 34), (35, 35), (36, 36), (37, 37), (38, 38), (39, 39), (40, 40), (41, 41), (42, 42), (43, 43), (44, 44), (45, 45), (46, 46), (47, 47), (48, 48), (49, 49), (50, 50), (51, 51), (52, 52), (53, 53), (54, 54), (55, 55), (56, 56), (57, 57), (58, 58), (59, 59), (60, 60), (61, 61), (62, 62), (63, 63), (64, 64), (65, 65), (66, 66), (67, 67), (68, 68), (69, 69), (70, 70), (71, 71), (72, 72), (73, 73), (74, 74), (75, 75), (76, 76), (77, 77), (78, 78), (79, 79), (80, 80), (81, 81), (82, 82), (83, 83), (84, 84), (85, 85), (86, 86), (87, 87), (88, 88), (89, 89), (90, 90), (91, 91), (92, 92), (93, 93), (94, 94), (95, 95), (96, 96), (97, 97), (98, 98), (99, 99), (100, 100), (101, 101), (102, 102), (103, 103), (104, 104), (105, 105), (106, 106), (107, 107), (108, 108), (109, 109), (110, 110), (111, 111), (112, 112), (113, 113), (114, 114), (115, 115), (116, 116), (117, 117), (118, 118), (119, 119), (120, 120), (121, 121), (122, 122), (123, 123), (124, 124), (125, 125), (126, 126), (127, 127), (128, 128), (129, 129), (130, 130), (131, 131), (132, 132), (133, 133), (134, 134), (135, 135), (136, 136), (137, 137), (138, 138), (139, 139), (140, 140), (141, 141), (142, 142), (143, 143), (144, 144), (145, 145), (146, 146), (147, 147), (148, 148), (149, 149), (150, 150), (151, 151), (152, 152), (153, 153), (154, 154), (155, 155), (156, 156), (157, 157), (158, 158), (159, 159), (160, 160), (161, 161), (162, 162), (163, 163), (164, 164), (165, 165), (166, 166), (167, 167), (168, 168), (169, 169), (170, 170), (171, 171), (172, 172), (173, 173), (174, 174), (175, 175), (176, 176), (177, 177), (178, 178), (179, 179), (180, 180), (181, 181), (182, 182), (183, 183), (184, 184), (185, 185), (186, 186), (187, 187), (188, 188), (189, 189), (190, 190), (191, 191), (192, 192), (193, 193), (194, 194), (195, 195), (196, 196), (197, 197), (198, 198), (199, 199), (200, 200), (201, 201), (202, 202), (203, 203), (204, 204), (205, 205), (206, 206), (207, 207), (208, 208), (209, 209), (210, 210), (211, 211), (212, 212), (213, 213), (214, 214), (215, 215), (216, 216), (217, 217), (218, 218), (219, 219), (220, 220), (221, 221), (222, 222), (223, 223), (224, 224), (225, 225), (226, 226), (227, 227), (228, 228), (229, 229), (230, 230), (231, 231), (232, 232), (233, 233), (234, 234), (235, 235), (236, 236), (237, 237), (238, 238), (239, 239), (240, 240), (241, 241), (242, 242), (243, 243), (244, 244), (245, 245), (246, 246), (247, 247), (248, 248), (249, 249), (250, 250), (251, 251), (252, 252), (253, 253), (254, 254), (255, 255), (256, 256), (257, 257), (258, 258), (259, 259), (260, 260), (261, 261), (262, 262), (263, 263), (264, 264), (265, 265), (266, 266), (267, 267), (268, 268), (269, 269), (270, 270), (271, 271), (272, 272), (273, 273), (274, 274), (275, 275), (276, 276), (277, 277), (278, 278), (279, 279), (280, 280), (281, 281), (282, 282), (283, 283), (284, 284), (285, 285), (286, 286), (287, 287), (288, 288), (289, 289), (290, 290), (291, 291), (292, 292), (293, 293), (294, 294), (295, 295), (296, 296), (297, 297), (298, 298), (299, 299), (300, 300), (301, 301), (302, 302), (303, 303), (304, 304), (305, 305), (306, 306), (307, 307), (308, 308), (309, 309), (310, 310), (311, 311), (312, 312), (313, 313), (314, 314), (315, 315), (316, 316), (317, 317), (318, 318), (319, 319), (320, 320), (321, 321), (322, 322), (323, 323), (324, 324), (325, 325), (326, 326), (327, 327), (328, 328), (329, 329), (330, 330), (331, 331), (332, 332), (333, 333), (334, 334), (335, 335), (336, 336), (337, 337), (338, 338), (339, 339), (340, 340), (341, 341), (342, 342), (343, 343), (344, 344), (345, 345), (346, 346), (347, 347), (348, 348), (349, 349), (350, 350), (351, 351), (352, 352), (353, 353), (354, 354), (355, 355), (356, 356), (357, 357), (358, 358), (359, 359), (360, 360), (361, 361), (362, 362), (363, 363), (364, 364), (365, 365), (366, 366), (367, 367), (368, 368), (369, 369), (370, 370), (371, 371), (372, 372), (373, 373), (374, 374), (375, 375), (376, 376), (377, 377), (378, 378), (379, 379), (380, 380), (381, 381), (382, 382), (383, 383), (384, 384), (385, 385), (386, 386), (387, 387), (388, 388), (389, 389), (390, 390), (391, 391), (392, 392), (393, 393), (394, 394), (395, 395), (396, 396), (397, 397), (398, 398), (399, 399), (400, 400), (401, 401), (402, 402), (403, 403), (404, 404), (405, 405), (406, 406), (407, 407), (408, 408), (409, 409), (410, 410), (411, 411), (412, 412), (413, 413), (414, 414), (415, 415), (416, 416), (417, 417), (418, 418), (419, 419), (420, 420), (421, 421), (422, 422), (423, 423), (424, 424), (425, 425), (426, 426), (427, 427), (428, 428), (429, 429), (430, 430), (431, 431), (432, 432), (433, 433), (434, 434), (435, 435), (436, 436), (437, 437), (438, 438), (439, 439), (440, 440), (441, 441), (442, 442), (443, 443), (444, 444), (445, 445), (446, 446), (447, 447), (448, 448), (449, 449), (450, 450), (451, 451), (452, 452), (453, 453), (454, 454), (455, 455), (456, 456), (457, 457), (458, 458), (459, 459), (460, 460), (461, 461), (462, 462), (463, 463), (464, 464), (465, 465), (466, 466), (467, 467), (468, 468), (469, 469), (470, 470), (471, 471), (472, 472), (473, 473), (474, 474), (475, 475), (476, 476), (477, 477), (478, 478), (479, 479), (480, 480), (481, 481), (482, 482), (483, 483), (484, 484), (485, 485), (486, 486), (487, 487), (488, 488), (489, 489), (490, 490), (491, 491), (492, 492), (493, 493), (494, 494), (495, 495), (496, 496), (497, 497), (498, 498), (499, 499), (500, 500), (501, 501), (502, 502), (503, 503), (504, 504), (505, 505), (506, 506), (507, 507), (508, 508), (509, 509), (510, 510), (511, 511), (512, 512), (513, 513), (514, 514), (515, 515), (516, 516), (517, 517), (518, 518), (519, 519), (520, 520), (521, 521), (522, 522), (523, 523), (524, 524), (525, 525), (526, 526), (527, 527), (528, 528), (529, 529), (530, 530), (531, 531), (532, 532), (533, 533), (534, 534), (535, 535), (536, 536), (537, 537), (538, 538), (539, 539), (540, 540), (541, 541), (542, 542), (543, 543), (544, 544), (545, 545), (546, 546), (547, 547), (548, 548), (549, 549), (550, 550), (551, 551), (552, 552), (553, 553), (554, 554), (555, 555), (556, 556), (557, 557), (558, 558), (559, 559), (560, 560), (561, 561), (562, 562), (563, 563), (564, 564), (565, 565), (566, 566), (567, 567), (568, 568), (569, 569), (570, 570), (571, 571), (572, 572), (573, 573), (574, 574), (575, 575), (576, 576), (577, 577), (578, 578), (579, 579), (580, 580), (581, 581), (582, 582), (583, 583), (584, 584), (585, 585), (586, 586), (587, 587), (588, 588), (589, 589), (590, 590), (591, 591), (592, 592), (593, 593), (594, 594), (595, 595), (596, 596), (597, 597), (598, 598), (599, 599), (600, 600), (601, 601), (602, 602), (603, 603), (604, 604), (605, 605), (606, 606), (607, 607), (608, 608), (609, 609), (610, 610), (611, 611), (612, 612), (613, 613), (614, 614), (615, 615), (616, 616), (617, 617), (618, 618), (619, 619), (620, 620), (621, 621), (622, 622), (623, 623), (624, 624), (625, 625), (626, 626), (627, 627), (628, 628), (629, 629), (630, 630), (631, 631), (632, 632), (633, 633), (634, 634), (635, 635), (636, 636), (637, 637), (638, 638), (639, 639), (640, 640), (641, 641), (642, 642), (643, 643), (644, 644), (645, 645), (646, 646), (647, 647), (648, 648), (649, 649), (650, 650), (651, 651), (652, 652), (653, 653), (654, 654), (655, 655), (656, 656), (657, 657), (658, 658), (659, 659), (660, 660), (661, 661), (662, 662), (663, 663), (664, 664), (665, 665), (666, 666), (667, 667), (668, 668), (669, 669), (670, 670), (671, 671), (672, 672), (673, 673), (674, 674), (675, 675), (676, 676), (677, 677), (678, 678), (679, 679), (680, 680), (681, 681), (682, 682), (683, 683), (684, 684), (685, 685), (686, 686), (687, 687), (688, 688), (689, 689), (690, 690), (691, 691), (692, 692), (693, 693), (694, 694), (695, 695), (696, 696), (697, 697), (698, 698), (699, 699), (700, 700), (701, 701), (702, 702), (703, 703), (704, 704), (705, 705), (706, 706), (707, 707), (708, 708), (709, 709), (710, 710), (711, 711), (712, 712), (713, 713), (714, 714), (715, 715), (716, 716), (717, 717), (718, 718), (719, 719), (720, 720), (721, 721), (722, 722), (723, 723), (724, 724), (725, 725), (726, 726), (727, 727), (728, 728), (729, 729), (730, 730), (731, 731), (732, 732), (733, 733), (734, 734), (735, 735), (736, 736), (737, 737), (738, 738), (739, 739), (740, 740), (741, 741), (742, 742), (743, 743), (744, 744), (745, 745), (746, 746), (747, 747), (748, 748), (749, 749), (750, 750), (751, 751), (752, 752), (753, 753), (754, 754), (755, 755), (756, 756), (757, 757), (758, 758), (759, 759), (760, 760), (761, 761), (762, 762), (763, 763), (764, 764), (765, 765), (766, 766), (767, 767), (768, 768), (769, 769), (770, 770), (771, 771), (772, 772), (773, 773), (774, 774), (775, 775), (776, 776), (777, 777), (778, 778), (779, 779), (780, 780), (781, 781), (782, 782), (783, 783), (784, 784), (785, 785), (786, 786), (787, 787), (788, 788), (789, 789), (790, 790), (791, 791), (792, 792), (793, 793), (794, 794), (795, 795), (796, 796), (797, 797), (798, 798), (799, 799), (800, 800), (801, 801), (802, 802), (803, 803), (804, 804), (805, 805), (806, 806), (807, 807), (808, 808), (809, 809), (810, 810), (811, 811), (812, 812), (813, 813), (814, 814), (815, 815), (816, 816), (817, 817), (818, 818), (819, 819), (820, 820), (821, 821), (822, 822), (823, 823), (824, 824), (825, 825), (826, 826), (827, 827), (828, 828), (829, 829), (830, 830), (831, 831), (832, 832), (833, 833), (834, 834), (835, 835), (836, 836), (837, 837), (838, 838), (839, 839), (840, 840), (841, 841), (842, 842), (843, 843), (844, 844), (845, 845), (846, 846), (847, 847), (848, 848), (849, 849), (850, 850), (851, 851), (852, 852), (853, 853), (854, 854), (855, 855), (856, 856), (857, 857), (858, 858), (859, 859), (860, 860), (861, 861), (862, 862), (863, 863), (864, 864), (865, 865), (866, 866), (867, 867), (868, 868), (869, 869), (870, 870), (871, 871), (872, 872), (873, 873), (874, 874), (875, 875), (876, 876), (877, 877), (878, 878), (879, 879), (880, 880), (881, 881), (882, 882), (883, 883), (884, 884), (885, 885), (886, 886), (887, 887), (888, 888), (889, 889), (890, 890), (891, 891), (892, 892), (893, 893), (894, 894), (895, 895), (896, 896), (897, 897), (898, 898), (899, 899), (900, 900), (901, 901), (902, 902), (903, 903), (904, 904), (905, 905), (906, 906), (907, 907), (908, 908), (909, 909), (910, 910), (911, 911), (912, 912), (913, 913), (914, 914), (915, 915), (916, 916), (917, 917), (918, 918), (919, 919), (920, 920), (921, 921), (922, 922), (923, 923), (924, 924), (925, 925), (926, 926), (927, 927), (928, 928), (929, 929), (930, 930), (931, 931), (932, 932), (933, 933), (934, 934), (935, 935), (936, 936), (937, 937), (938, 938), (939, 939), (940, 940), (941, 941), (942, 942), (943, 943), (944, 944), (945, 945), (946, 946), (947, 947), (948, 948), (949, 949), (950, 950), (951, 951), (952, 952), (953, 953), (954, 954), (955, 955), (956, 956), (957, 957), (958, 958), (959, 959), (960, 960), (961, 961), (962, 962), (963, 963), (964, 964), (965, 965), (966, 966), (967, 967), (968, 968), (969, 969), (970, 970), (971, 971), (972, 972), (973, 973), (974, 974), (975, 975), (976, 976), (977, 977), (978, 978), (979, 979), (980, 980), (981, 981), (982, 982), (983, 983), (984, 984), (985, 985), (986, 986), (987, 987), (988, 988), (989, 989), (990, 990), (991, 991), (992, 992), (993, 993), (994, 994), (995, 995), (996, 996), (997, 997), (998, 998), (999, 999), (1000, 1000), (1001, 1001), (1002, 1002), (1003, 1003), (1004, 1004), (1005, 1005), (1006, 1006), (1007, 1007), (1008, 1008), (1009, 1009), (1010, 1010), (1011, 1011), (1012, 1012), (1013, 1013), (1014, 1014), (1015, 1015), (1016, 1016), (1017, 1017), (1018, 1018), (1019, 1019), (1020, 1020), (1021, 1021), (1022, 1022), (1023, 1023), (1024, 1024), (1025, 1025), (1026, 1026), (1027, 1027), (1028, 1028), (1029, 1029), (1030, 1030), (1031, 1031), (1032, 1032), (1033, 1033), (1034, 1034), (1035, 1035), (1036, 1036), (1037, 1037), (1038, 1038), (1039, 1039), (1040, 1040), (1041, 1041), (1042, 1042), (1043, 1043), (1044, 1044), (1045, 1045), (1046, 1046), (1047, 1047), (1048, 1048), (1049, 1049), (1050, 1050), (1051, 1051), (1052, 1052), (1053, 1053), (1054, 1054), (1055, 1055), (1056, 1056), (1057, 1057), (1058, 1058), (1059, 1059), (1060, 1060), (1061, 1061), (1062, 1062), (1063, 1063), (1064, 1064), (1065, 1065), (1066, 1066), (1067, 1067), (1068, 1068), (1069, 1069), (1070, 1070), (1071, 1071), (1072, 1072), (1073, 1073), (1074, 1074), (1075, 1075), (1076, 1076), (1077, 1077), (1078, 1078), (1079, 1079), (1080, 1080), (1081, 1081), (1082, 1082), (1083, 1083), (1084, 1084), (1085, 1085), (1086, 1086), (1087, 1087), (1088, 1088), (1089, 1089), (1090, 1090), (1091, 1091), (1092, 1092), (1093, 1093), (1094, 1094), (1095, 1095), (1096, 1096), (1097, 1097), (1098, 1098), (1099, 1099), (1100, 1100), (1101, 1101), (1102, 1102), (1103, 1103), (1104, 1104), (1105, 1105), (1106, 1106), (1107, 1107), (1108, 1108), (1109, 1109), (1110, 1110), (1111, 1111), (1112, 1112), (1113, 1113), (1114, 1114), (1115, 1115), (1116, 1116), (1117, 1117), (1118, 1118), (1119, 1119), (1120, 1120), (1121, 1121), (1122, 1122), (1123, 1123), (1124, 1124), (1125, 1125), (1126, 1126), (1127, 1127), (1128, 1128), (1129, 1129), (1130, 1130), (1131, 1131), (1132, 1132), (1133, 1133), (1134, 1134), (1135, 1135), (1136, 1136), (1137, 1137), (1138, 1138), (1139, 1139), (1140, 1140), (1141, 1141), (1142, 1142), (1143, 1143), (1144, 1144), (1145, 1145), (1146, 1146), (1147, 1147), (1148, 1148), (1149, 1149), (1150, 1150), (1151, 1151), (1152, 1152), (1153, 1153), (1154, 1154), (1155, 1155), (1156, 1156), (1157, 1157), (1158, 1158), (1159, 1159), (1160, 1160), (1161, 1161), (1162, 1162), (1163, 1163), (1164, 1164), (1165, 1165), (1166, 1166), (1167, 1167), (1168, 1168), (1169, 1169), (1170, 1170), (1171, 1171), (1172, 1172), (1173, 1173), (1174, 1174), (1175, 1175), (1176, 1176), (1177, 1177), (1178, 1178), (1179, 1179), (1180, 1180), (1181, 1181), (1182, 1182), (1183, 1183), (1184, 1184), (1185, 1185), (1186, 1186), (1187, 1187), (1188, 1188), (1189, 1189), (1190, 1190), (1191, 1191), (1192, 1192), (1193, 1193), (1194, 1194), (1195, 1195), (1196, 1196), (1197, 1197), (1198, 1198), (1199, 1199), (1200, 1200), (1201, 1201), (1202, 1202), (1203, 1203), (1204, 1204), (1205, 1205), (1206, 1206), (1207, 1207), (1208, 1208), (1209, 1209), (1210, 1210), (1211, 1211), (1212, 1212), (1213, 1213), (1214, 1214), (1215, 1215), (1216, 1216), (1217, 1217), (1218, 1218), (1219, 1219), (1220, 1220), (1221, 1221), (1222, 1222), (1223, 1223), (1224, 1224), (1225, 1225), (1226, 1226), (1227, 1227), (1228, 1228), (1229, 1229), (1230, 1230), (1231, 1231), (1232, 1232), (1233, 1233), (1234, 1234), (1235, 1235), (1236, 1236), (1237, 1237), (1238, 1238), (1239, 1239), (1240, 1240), (1241, 1241), (1242, 1242), (1243, 1243), (1244, 1244), (1245, 1245), (1246, 1246), (1247, 1247), (1248, 1248), (1249, 1249), (1250, 1250), (1251, 1251), (1252, 1252), (1253, 1253), (1254, 1254), (1255, 1255), (1256, 1256), (1257, 1257), (1258, 1258), (1259, 1259), (1260, 1260), (1261, 1261), (1262, 1262), (1263, 1263), (1264, 1264), (1265, 1265), (1266, 1266), (1267, 1267), (1268, 1268), (1269, 1269), (1270, 1270), (1271, 1271), (1272, 1272), (1273, 1273), (1274, 1274), (1275, 1275), (1276, 1276), (1277, 1277), (1278, 1278), (1279, 1279), (1280, 1280), (1281, 1281), (1282, 1282), (1283, 1283), (1284, 1284), (1285, 1285), (1286, 1286), (1287, 1287), (1288, 1288), (1289, 1289), (1290, 1290), (1291, 1291), (1292, 1292), (1293, 1293), (1294, 1294), (1295, 1295), (1296, 1296), (1297, 1297), (1298, 1298), (1299, 1299), (1300, 1300), (1301, 1301), (1302, 1302), (1303, 1303), (1304, 1304), (1305, 1305), (1306, 1306), (1307, 1307), (1308, 1308), (1309, 1309), (1310, 1310), (1311, 1311), (1312, 1312), (1313, 1313), (1314, 1314), (1315, 1315), (1316, 1316), (1317, 1317), (1318, 1318), (1319, 1319), (1320, 1320), (1321, 1321), (1322, 1322), (1323, 1323), (1324, 1324), (1325, 1325), (1326, 1326), (1327, 1327), (1328, 1328), (1329, 1329), (1330, 1330), (1331, 1331), (1332, 1332), (1333, 1333), (1334, 1334), (1335, 1335), (1336, 1336), (1337, 1337), (1338, 1338), (1339, 1339), (1340, 1340), (1341, 1341), (1342, 1342), (1343, 1343), (1344, 1344), (1345, 1345), (1346, 1346), (1347, 1347), (1348, 1348), (1349, 1349), (1350, 1350), (1351, 1351), (1352, 1352), (1353, 1353), (1354, 1354), (1355, 1355), (1356, 1356), (1357, 1357), (1358, 1358), (1359, 1359), (1360, 1360), (1361, 1361), (1362, 1362), (1363, 1363), (1364, 1364), (1365, 1365), (1366, 1366), (1367, 1367), (1368, 1368), (1369, 1369), (1370, 1370), (1371, 1371), (1372, 1372), (1373, 1373), (1374, 1374), (1375, 1375), (1376, 1376), (1377, 1377), (1378, 1378), (1379, 1379), (1380, 1380), (1381, 1381), (1382, 1382), (1383, 1383), (1384, 1384), (1385, 1385), (1386, 1386), (1387, 1387), (1388, 1388), (1389, 1389), (1390, 1390), (1391, 1391), (1392, 1392), (1393, 1393), (1394, 1394), (1395, 1395), (1396, 1396), (1397, 1397), (1398, 1398), (1399, 1399), (1400, 1400), (1401, 1401), (1402, 1402), (1403, 1403), (1404, 1404), (1405, 1405), (1406, 1406), (1407, 1407), (1408, 1408), (1409, 1409), (1410, 1410), (1411, 1411), (1412, 1412), (1413, 1413), (1414, 1414), (1415, 1415), (1416, 1416), (1417, 1417), (1418, 1418), (1419, 1419), (1420, 1420), (1421, 1421), (1422, 1422), (1423, 1423), (1424, 1424), (1425, 1425), (1426, 1426), (1427, 1427), (1428, 1428), (1429, 1429), (1430, 1430), (1431, 1431), (1432, 1432), (1433, 1433), (1434, 1434), (1435, 1435), (1436, 1436), (1437, 1437), (1438, 1438), (1439, 1439), (1440, 1440), (1441, 1441), (1442, 1442), (1443, 1443), (1444, 1444), (1445, 1445), (1446, 1446), (1447, 1447), (1448, 1448), (1449, 1449), (1450, 1450), (1451, 1451), (1452, 1452), (1453, 1453), (1454, 1454), (1455, 1455), (1456, 1456), (1457, 1457), (1458, 1458), (1459, 1459), (1460, 1460), (1461, 1461), (1462, 1462), (1463, 1463), (1464, 1464), (1465, 1465), (1466, 1466), (1467, 1467), (1468, 1468), (1469, 1469), (1470, 1470), (1471, 1471), (1472, 1472), (1473, 1473), (1474, 1474), (1475, 1475), (1476, 1476), (1477, 1477), (1478, 1478), (1479, 1479), (1480, 1480), (1481, 1481), (1482, 1482), (1483, 1483), (1484, 1484), (1485, 1485), (1486, 1486), (1487, 1487), (1488, 1488), (1489, 1489), (1490, 1490), (1491, 1491), (1492, 1492), (1493, 1493), (1494, 1494), (1495, 1495), (1496, 1496), (1497, 1497), (1498, 1498), (1499, 1499), (1500, 1500), (1501, 1501), (1502, 1502), (1503, 1503), (1504, 1504), (1505, 1505), (1506, 1506), (1507, 1507), (1508, 1508), (1509, 1509), (1510, 1510), (1511, 1511), (1512, 1512), (1513, 1513), (1514, 1514), (1515, 1515), (1516, 1516), (1517, 1517), (1518, 1518), (1519, 1519), (1520, 1520), (1521, 1521), (1522, 1522), (1523, 1523), (1524, 1524), (1525, 1525), (1526, 1526), (1527, 1527), (1528, 1528), (1529, 1529), (1530, 1530), (1531, 1531), (1532, 1532), (1533, 1533), (1534, 1534), (1535, 1535), (1536, 1536), (1537, 1537), (1538, 1538), (1539, 1539), (1540, 1540), (1541, 1541), (1542, 1542), (1543, 1543), (1544, 1544), (1545, 1545), (1546, 1546), (1547, 1547), (1548, 1548), (1549, 1549), (1550, 1550), (1551, 1551), (1552, 1552), (1553, 1553), (1554, 1554), (1555, 1555), (1556, 1556), (1557, 1557), (1558, 1558), (1559, 1559), (1560, 1560), (1561, 1561), (1562, 1562), (1563, 1563), (1564, 1564), (1565, 1565), (1566, 1566), (1567, 1567), (1568, 1568), (1569, 1569), (1570, 1570), (1571, 1571), (1572, 1572), (1573, 1573), (1574, 1574), (1575, 1575), (1576, 1576), (1577, 1577), (1578, 1578), (1579, 1579), (1580, 1580), (1581, 1581), (1582, 1582), (1583, 1583), (1584, 1584), (1585, 1585), (1586, 1586), (1587, 1587), (1588, 1588), (1589, 1589), (1590, 1590), (1591, 1591), (1592, 1592), (1593, 1593), (1594, 1594), (1595, 1595), (1596, 1596), (1597, 1597), (1598, 1598), (1599, 1599), (1600, 1600), (1601, 1601), (1602, 1602), (1603, 1603), (1604, 1604), (1605, 1605), (1606, 1606), (1607, 1607), (1608, 1608), (1609, 1609), (1610, 1610), (1611, 1611), (1612, 1612), (1613, 1613), (1614, 1614), (1615, 1615), (1616, 1616), (1617, 1617), (1618, 1618), (1619, 1619), (1620, 1620), (1621, 1621), (1622, 1622), (1623, 1623), (1624, 1624), (1625, 1625), (1626, 1626), (1627, 1627), (1628, 1628), (1629, 1629), (1630, 1630), (1631, 1631), (1632, 1632), (1633, 1633), (1634, 1634), (1635, 1635), (1636, 1636), (1637, 1637), (1638, 1638), (1639, 1639), (1640, 1640), (1641, 1641), (1642, 1642), (1643, 1643), (1644, 1644), (1645, 1645), (1646, 1646), (1647, 1647), (1648, 1648), (1649, 1649), (1650, 1650), (1651, 1651), (1652, 1652), (1653, 1653), (1654, 1654), (1655, 1655), (1656, 1656), (1657, 1657), (1658, 1658), (1659, 1659), (1660, 1660), (1661, 1661), (1662, 1662), (1663, 1663), (1664, 1664), (1665, 1665), (1666, 1666), (1667, 1667), (1668, 1668), (1669, 1669), (1670, 1670), (1671, 1671), (1672, 1672), (1673, 1673), (1674, 1674), (1675, 1675), (1676, 1676), (1677, 1677), (1678, 1678), (1679, 1679), (1680, 1680), (1681, 1681), (1682, 1682), (1683, 1683), (1684, 1684), (1685, 1685), (1686, 1686), (1687, 1687), (1688, 1688), (1689, 1689), (1690, 1690), (1691, 1691), (1692, 1692), (1693, 1693), (1694, 1694), (1695, 1695), (1696, 1696), (1697, 1697), (1698, 1698), (1699, 1699), (1700, 1700), (1701, 1701), (1702, 1702), (1703, 1703), (1704, 1704), (1705, 1705), (1706, 1706), (1707, 1707), (1708, 1708), (1709, 1709), (1710, 1710), (1711, 1711), (1712, 1712), (1713, 1713), (1714, 1714), (1715, 1715), (1716, 1716), (1717, 1717), (1718, 1718), (1719, 1719), (1720, 1720), (1721, 1721), (1722, 1722), (1723, 1723), (1724, 1724), (1725, 1725), (1726, 1726), (1727, 1727), (1728, 1728), (1729, 1729), (1730, 1730), (1731, 1731), (1732, 1732), (1733, 1733), (1734, 1734), (1735, 1735), (1736, 1736), (1737, 1737), (1738, 1738), (1739, 1739), (1740, 1740), (1741, 1741), (1742, 1742), (1743, 1743), (1744, 1744), (1745, 1745), (1746, 1746), (1747, 1747), (1748, 1748), (1749, 1749), (1750, 1750), (1751, 1751), (1752, 1752), (1753, 1753), (1754, 1754), (1755, 1755), (1756, 1756), (1757, 1757), (1758, 1758), (1759, 1759), (1760, 1760), (1761, 1761), (1762, 1762), (1763, 1763), (1764, 1764), (1765, 1765), (1766, 1766), (1767, 1767), (1768, 1768), (1769, 1769), (1770, 1770), (1771, 1771), (1772, 1772), (1773, 1773), (1774, 1774), (1775, 1775), (1776, 1776), (1777, 1777), (1778, 1778), (1779, 1779), (1780, 1780), (1781, 1781), (1782, 1782), (1783, 1783), (1784, 1784), (1785, 1785), (1786, 1786), (1787, 1787), (1788, 1788), (1789, 1789), (1790, 1790), (1791, 1791), (1792, 1792), (1793, 1793), (1794, 1794), (1795, 1795), (1796, 1796), (1797, 1797), (1798, 1798), (1799, 1799), (1800, 1800), (1801, 1801), (1802, 1802), (1803, 1803), (1804, 1804), (1805, 1805), (1806, 1806), (1807, 1807), (1808, 1808), (1809, 1809), (1810, 1810), (1811, 1811), (1812, 1812), (1813, 1813), (1814, 1814), (1815, 1815), (1816, 1816), (1817, 1817), (1818, 1818), (1819, 1819), (1820, 1820), (1821, 1821), (1822, 1822), (1823, 1823), (1824, 1824), (1825, 1825), (1826, 1826), (1827, 1827), (1828, 1828), (1829, 1829), (1830, 1830), (1831, 1831), (1832, 1832), (1833, 1833), (1834, 1834), (1835, 1835), (1836, 1836), (1837, 1837), (1838, 1838), (1839, 1839), (1840, 1840), (1841, 1841), (1842, 1842), (1843, 1843), (1844, 1844), (1845, 1845), (1846, 1846), (1847, 1847), (1848, 1848), (1849, 1849), (1850, 1850), (1851, 1851), (1852, 1852), (1853, 1853), (1854, 1854), (1855, 1855), (1856, 1856), (1857, 1857), (1858, 1858), (1859, 1859), (1860, 1860), (1861, 1861), (1862, 1862), (1863, 1863), (1864, 1864), (1865, 1865), (1866, 1866), (1867, 1867), (1868, 1868), (1869, 1869), (1870, 1870), (1871, 1871), (1872, 1872), (1873, 1873), (1874, 1874), (1875, 1875), (1876, 1876), (1877, 1877), (1878, 1878), (1879, 1879), (1880, 1880), (1881, 1881), (1882, 1882), (1883, 1883), (1884, 1884), (1885, 1885), (1886, 1886), (1887, 1887), (1888, 1888), (1889, 1889), (1890, 1890), (1891, 1891), (1892, 1892), (1893, 1893), (1894, 1894), (1895, 1895), (1896, 1896), (1897, 1897), (1898, 1898), (1899, 1899), (1900, 1900), (1901, 1901), (1902, 1902), (1903, 1903), (1904, 1904), (1905, 1905), (1906, 1906), (1907, 1907), (1908, 1908), (1909, 1909), (1910, 1910), (1911, 1911), (1912, 1912), (1913, 1913), (1914, 1914), (1915, 1915), (1916, 1916), (1917, 1917), (1918, 1918), (1919, 1919), (1920, 1920), (1921, 1921), (1922, 1922), (1923, 1923), (1924, 1924), (1925, 1925), (1926, 1926), (1927, 1927), (1928, 1928), (1929, 1929), (1930, 1930), (1931, 1931), (1932, 1932), (1933, 1933), (1934, 1934), (1935, 1935), (1936, 1936), (1937, 1937), (1938, 1938), (1939, 1939), (1940, 1940), (1941, 1941), (1942, 1942), (1943, 1943), (1944, 1944), (1945, 1945), (1946, 1946), (1947, 1947), (1948, 1948), (1949, 1949), (1950, 1950), (1951, 1951), (1952, 1952), (1953, 1953), (1954, 1954), (1955, 1955), (1956, 1956), (1957, 1957), (1958, 1958), (1959, 1959), (1960, 1960), (1961, 1961), (1962, 1962), (1963, 1963), (1964, 1964), (1965, 1965), (1966, 1966), (1967, 1967), (1968, 1968), (1969, 1969), (1970, 1970), (1971, 1971), (1972, 1972), (1973, 1973), (1974, 1974), (1975, 1975), (1976, 1976), (1977, 1977), (1978, 1978), (1979, 1979), (1980, 1980), (1981, 1981), (1982, 1982), (1983, 1983), (1984, 1984), (1985, 1985), (1986, 1986), (1987, 1987), (1988, 1988), (1989, 1989), (1990, 1990), (1991, 1991), (1992, 1992), (1993, 1993), (1994, 1994), (1995, 1995), (1996, 1996), (1997, 1997), (1998, 1998), (1999, 1999), (2000, 2000), (2001, 2001), (2002, 2002), (2003, 2003), (2004, 2004), (2005, 2005), (2006, 2006), (2007, 2007), (2008, 2008), (2009, 2009), (2010, 2010), (2011, 2011), (2012, 2012), (2013, 2013), (2014, 2014), (2015, 2015), (2016, 2016), (2017, 2017), (2018, 2018), (2019, 2019), (2020, 2020), (2021, 2021), (2022, 2022)], default=2022)),
                ('publisher', models.CharField(max_length=50)),
                ('image_s', models.URLField(verbose_name='Small Image')),
                ('image_m', models.URLField(verbose_name='Medium Image')),
                ('image_l', models.URLField(verbose_name='Large Image')),
            ],
        ),
        migrations.CreateModel(
            name='Rating',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('userID', models.CharField(max_length=10)),
                ('rating', models.IntegerField()),
                ('book', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='bookrec.book')),
            ],
        ),
    ]
