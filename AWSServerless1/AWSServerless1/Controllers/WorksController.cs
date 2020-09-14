using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using AWSServerless1.Models;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AWSServerless1.Controllers
{
    [Route("api/[controller]")]
    public class WorksController : ControllerBase
    {
        private readonly IDynamoDBContext _dynamoDbContext;

        public WorksController(IDynamoDBContext dynamoDbContext)
        {
            _dynamoDbContext = dynamoDbContext;
        }

        [HttpGet]
        public async Task<ActionResult<List<Work>>> Get()
        {           
            var response = _dynamoDbContext.QueryAsync<Work>("1");
            //var response = _dynamoDbContext.ScanAsync<Work>(
            //    new[] { new ScanCondition("ConsumerId", ScanOperator.Equal, "1") });

            return await response.GetRemainingAsync();
        }
    }
}
