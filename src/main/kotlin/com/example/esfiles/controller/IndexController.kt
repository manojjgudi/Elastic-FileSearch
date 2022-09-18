package com.example.esfiles.controller

import com.example.esfiles.filestorage.FileStorage
import com.example.esfiles.model.FileInfo
import org.apache.http.HttpHost
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.WildcardQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.Resource
import org.springframework.http.HttpHeaders
import org.springframework.http.ResponseEntity
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.*
import org.springframework.web.multipart.MultipartFile
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder
import java.io.File
import java.util.*
import java.util.stream.Collectors


/*
elastic search attachment pipeline
PUT _ingest/pipeline/attachment-pipeline
{
  "description" : "Extract attachment information from arrays",
  "processors" : [
    {
      "foreach": {
        "field": "attachments",
        "processor": {
          "attachment": {
            "target_field": "_ingest._value.attachment",
            "field": "_ingest._value.data"
          }
        }
      }
    }
  ]
}

query to get docs that returns indexes(files) containing the search_keyword
GET /{index_name}/_search
{
  "query": {
    "wildcard": {
      "attachments.attachment.content": {
        "value": "*search_keyword*"
      }
    }
  }
}

 */

@RestController

class IndexController {

    @Autowired
    lateinit var fileStorage: FileStorage
    @RequestMapping("/")
    fun index(): String? {
        return "upload"
    }

    @PostMapping("/upload")
    fun upload(@RequestPart("file")file: MultipartFile, @RequestPart("fileName") fileName: String): IndexResponse {
        fileStorage.store(file)
        file.transferTo(File("es-files/src/main/resources/temp-files/$fileName"))
        val restClient = RestHighLevelClient(
            RestClient.builder(
                HttpHost("localhost", 9200)
            )
        )
        val fileNameFormatted = fileName.removeSuffix(".pdf").toLowerCase()
        val request = CreateIndexRequest(fileNameFormatted)
        if(!restClient.indices().exists(GetIndexRequest().indices(fileNameFormatted), RequestOptions.DEFAULT)){
            restClient.indices().create(request, RequestOptions.DEFAULT)
        }
        val fileContent = File("es-files/src/main/resources/temp-files/$fileName").readBytes()
        val encoder: Base64.Encoder = Base64.getEncoder()
        val encoded: String = encoder.encodeToString(fileContent)
        val indexRequest = IndexRequest(fileNameFormatted)
        indexRequest.pipeline = "attachment-pipeline"
        val attachment = mapOf("fileName" to fileNameFormatted, "data" to encoded)
        val jsonMap = mutableMapOf<String,Any>()
        jsonMap["attachments"] = listOf(attachment)
        indexRequest.source(jsonMap)
        val indexResponse: IndexResponse = restClient.index(indexRequest, RequestOptions.DEFAULT)
        return indexResponse
    }

    @PostMapping("/download")
    fun download(
        model: Model,
        @RequestParam("keyword") keyword: String?
    ): String {
        val restClient = RestHighLevelClient(
            RestClient.builder(
                HttpHost("localhost", 9200)
            )
        )
        println(keyword)
        val res = mutableSetOf<String>()
        val request = GetIndexRequest().indices("*")
        val response = restClient.indices().get(request, RequestOptions.DEFAULT)
        val indices: Array<String> = response.indices
        for (index in indices) {
            val searchRequest = SearchRequest(index)
            val searchSourceBuilder = SearchSourceBuilder()
            val query: QueryBuilder =
                QueryBuilders.boolQuery().should(WildcardQueryBuilder("attachments.attachment.content.keyword", "*$keyword*"))
            searchSourceBuilder.query(query)
            searchRequest.source(searchSourceBuilder)
                val searchResponse = restClient
                    .search(searchRequest, RequestOptions.DEFAULT)
                val hits = searchResponse.hits
                val searchHits = hits.hits
                for (hit in searchHits) {
                    val sourceAsMap = hit.sourceAsMap
                    res.add(sourceAsMap["attachments.fileName"].toString())
            }
        }
        val fileInfos: List<FileInfo> = fileStorage.loadFiles().map{
                path -> FileInfo(path.getFileName().toString(),
            MvcUriComponentsBuilder.fromMethodName(IndexController::class.java,
                "downloadFile", path.getFileName().toString()).build().toString())
        }.collect(Collectors.toList())
        val fileInfoFiltered = fileInfos.filter { it.filename in res }
        for( file in res) {
            model.addAttribute("files", fileInfoFiltered)
        }
        return "templates/listfiles"
    }

    @GetMapping("/{filename}")
    fun downloadFile(@PathVariable filename: String): ResponseEntity<Resource> {
        val file = fileStorage.loadFile(filename)
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file.getFilename() + "\"")
            .body(file);
    }
}