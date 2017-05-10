var page = {};
var api = {};
var base = {};
base.data_conn = function(path,rqd,backfn,type){
    type = (type?type.toLocaleLowerCase():false)
	if(type=="post"){
		rqd = JSON.stringify();
	}
	$.ajax({
        type: type||"get",
        url: path,
        dataType: "json",
        contentType: "application/json; charset=UTF-8",
        data: rqd,
        timeout: 100000,
        cache: false, 
        async: false,
        statusCode: {
        	
	    },
        beforeSend: function(XMLHttpRequest){
            //设置header参数
        	// XMLHttpRequest.withCredentials = true;
         	// XMLHttpRequest.setRequestHeader("myCookie",document.cookie);
        },
        success: function(data, code, xhr){
            if (data != null) {
                if (backfn != undefined && typeof(backfn) == "function") {
                    backfn(data, code,xhr);
                }
            }
        },
        error: function(XMLHttpRequest, error,option){
            if (error == "timeout") {
                console.log("请求超时：请求系统返回数据超时！请稍候再试吧…");
            }   
        }
    })
}
api.domain = 'http://127.0.0.1:8080/Users/a000/PycharmProjects/news_api_ml_board/wechat-show/api/';
//  左边数据列表
api.data_list = "data_list.json";
//  右边数据列表
api.data_info = "data_info.json";
page.UI = function(){
	$(".page-container").css("height",$(window).height()-100)
}
page.side_selected = function(){
	$(this).parent().children('li').removeClass('active');
	$(this).addClass('active');

    //获取单个微信号内容列表
	var url = api.domain + api.data_info;
	var request_data = {};
	request_data.name = $(this).attr("data-name");
	var url = "http://120.55.88.11:9999/news_process/GetAdsOfOneWechat"
	base.data_conn(url, request_data, page.show_data_info,'get')
}
page.box_change = function(){
	//base.data_conn('../api/data_info.json',{},page.box_change_back,'get')
	var v2 = $(this).prop('checked')
	var para = $(this).attr('para')
	console.log(para)
	var url = "http://120.55.88.11:9999/news_process/ModifyNewsAdsResults";
	var request_data = {};
	if(v2){
	    request_data['type'] = 'add'
	}
	else{
	    request_data['type'] = 'delete'
	}
	request_data['para'] = para
	base.data_conn(url,request_data, page.box_change_back,'get')
}
//page.box_change_back = function(data){
page.box_change_back = function(){
	console.log("修改成功")
}
// 判断是否是第一次请求
page.isFirst = true;
page.pageSize = 13;
//获取微信号列表
page.get_data_list = function(isfirst,pageSize,page_num){
	//var url = api.domain + api.data_list;
	//base.data_conn('../api/data_list.json',{},page.show_data_list,'get')
	page.isFirst = isfirst;
	//var url = "../api/data_list.json",request_data = {};
	var url = "http://120.55.88.11:9999/news_process/GetModifiedWechatNames"
	var request_data = {}
	request_data.pageSize = pageSize;
	request_data.page = page_num;
	console.log(url)
	base.data_conn(url,request_data,page.show_data_list,'get')

}
page.show_data_list = function(data){
	var list = data.list,html = '';
	for(var l in list){
		html += '<li '+(l==0?'class="active"':'')+' data-name='+list[l]+' >' + list[l] + '</li>';

	}
	if(page.isFirst){
		$("#pageDiv").removeData('bs.pagy');
	    $("#pageDiv > ul").empty();
	    $("#pageDiv").pagy({
	        currentPage: 1,
	        totalPages: Math.ceil(data.total_num/page.pageSize),
	        innerWindow: 1,
	        outerWindow: 1,
	        first: '',
	        prev: '<',
	        next: '>',
	        last: '',
	        gap: '..',
	        truncate: false,
	        page: function(p) {
				page.get_data_list(false,page.pageSize,p);
	            return true;
	        }
	    })
	}
	$(".data-list").html(html);
}
//返回页面
page.show_data_info = function(data){
    var list = data.list
    var name = data.name
	var html='';
	for(var l in list){
		html += '<tr>';
		//html += '<td> <input type="checkbox" data-name="'+ list[l].name +'" '+(list[l].check==1?"checked":"")+' /> </td>';
		html += '<td> <input para="'+name + ':'+list[l][0] + ':' + list[l][1]+'" type="checkbox" '+(true?"checked":"")+' /> </td>';
		html += '<td>' + list[l][0] + ': ' + list[l][1] + '</td>';
		html += '<td>' + list[l][2] + '</td>';
		html += '</tr>'
	}
	$(".data-info").html(html);
}
// 提交修改
page.submit_checked = function(){
	var url = "http://120.55.88.11:9999/news_process/SaveAdsModify"
	var request_data = {};
	//request_data.names = 'pageNo';
	base.data_conn(url,request_data,page.submit_back,'get')
}
page.submit_back = function(data){
	//alert(data)
}

$(function() {
    //点击微信号
    $(".data-list").on('click','li',page.side_selected);
	//点击checkbox
    $(".data-info").on('change','input',page.box_change);
    $("#sublime-edit").click(page.submit_checked)
    //
    page.get_data_list(true,page.pageSize,1);
    page.UI();
});
