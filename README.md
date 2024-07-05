# Project conclusion
Project named TVCP(T Video content platform)

has ability such as:
- Receive video upload trigger from HBase by Flink CDC
- Use Flink CEP/Join check whether video upload success
- Retrieve video raw features
- Compress video features by Protobuf
- Statistics video upload info
    - Upload video per second
    - Statistics video region
    - Statistics author's basic information like:age, gender, region



The overall architecture diagram is below:

![image-20240705164210077](https://pic-1306533678.cos.ap-nanjing.myqcloud.com/uPic/image-20240705164210077.png)