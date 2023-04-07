# 세미프로젝트 : 앱광고 유저 로그데이터 실시간으로 처리하기

> 시나리오

### 1.1 요구사항

1. mobile앱에서 광고를 요청시, 광고 데이터를 받는다.
2. 광고를 유저가 보면 impression data를 발생시킨다.
3. impression 데이터는 광고를 식별하기 위한 주요정보가 담겨있다.
    1. impId
    2. requestId
    3. adId
    4. userId
    5. deviceId
    6. inventoryId
4. 유저가 광고를 클릭하면 클릭데이터를 발생시킨다.
5. 클릭데이터는 impression Data이후 발생하는 데이터로써, 필수적인 정보만을 생성한다.
    1. impId
    2. clickURL

1. 정상 impression은 다음조건으로 판단한다.
    1. 한번 발생한 impId는 정해진 기간동안만 유효하다.
    2. 정해진기간은 정책에 따라 설정가능하다.
    
2. 정상 click event는 다음 조건으로 판단한다.
    1. 같은 impId인 impression이  impression기간동안 발생한적이 있어야한다.
        
        → click만 되었다는 것은 적절한 경로로 들어온것이 아니기때문에 관심대상이 아니다.
        
    2. 같은 impId로 발생한 click은 click기간 동안 하나만 유효하다.
    3. impression기간 및 click기간은 설정이가능하다.
3. 실시간 정상 click event는 adId기준으로 count한 결과를 확인할 수 있어야한다.


> 아키텍처

![Untitled](https://user-images.githubusercontent.com/56438131/230126917-3a211c9f-b99c-4fe9-bf21-f886eb368370.png)






