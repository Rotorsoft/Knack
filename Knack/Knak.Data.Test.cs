using Knak;
using Knak.Test;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;

namespace Knak.Data.Tests {
	#region Models

	public class EmployeeInfo : ICommand {
		public string GetScript() { return null; } // dummy for testing 

		[Output]
		public int EmployeeID { get; set; }

		[Parameter(10)]
		public string FirstName { get; set; }

		[Parameter(20)]
		public string LastName { get; set; }

		[Parameter(30, true)]
		public string Title { get; set; }

		public DateTime BirthDate { get; set; }
		public DateTime HireDate { get; set; }
		public int VacationHours { get; set; }

		[Parameter(1, true)]
		public string Sex { get; set; }

		public string Email { get; set; }
		public string phone { get; set; }
		public string ContactSuffix { get; set; }

		public static EmployeeInfo CreateSample1() {
			return new EmployeeInfo {
				FirstName = "John",
				LastName = "Doe",
				Title = "Missing Person",
				BirthDate = new DateTime(1980, 1, 1),
				HireDate = new DateTime(2000, 1, 1)
			};
		}
	}

	public class Currency {
		public Currency() { }

		[Required]
		[StringLength(3)]
		public string CurrencyCode { get; set; }
		[Required]
		[StringLength(50)]
		public string Name { get; set; }
		public DateTime ModifiedDate { get; set; }
	}

	public class ShipMethod {
		public ShipMethod() { }

		public Int32 ShipMethodID { get; set; }
		[Required]
		[StringLength(50)]
		public string Name { get; set; }
		public Decimal ShipBase { get; set; }
		public Decimal ShipRate { get; set; }
		public Guid rowguid { get; set; }
		public DateTime ModifiedDate { get; set; }
	}

	public class SalesOrderHeader {
		public SalesOrderHeader() {
			// Create details
			OrderDetails = new List<SalesOrderDetail>();
		}

		public Int32 SalesOrderID { get; set; }
		public Byte RevisionNumber { get; set; }
		public DateTime OrderDate { get; set; }
		public DateTime DueDate { get; set; }
		public DateTime? ShipDate { get; set; }
		public Byte Status { get; set; }
		public Boolean OnlineOrderFlag { get; set; }
		[Required]
		[StringLength(25)]
		public string SalesOrderNumber { get; set; }
		[StringLength(25)]
		public string PurchaseOrderNumber { get; set; }
		[StringLength(15)]
		public string AccountNumber { get; set; }
		public Int32 CustomerID { get; set; }
		public Int32? SalesPersonID { get; set; }
		public Int32? TerritoryID { get; set; }
		public Int32 BillToAddressID { get; set; }
		public Int32 ShipToAddressID { get; set; }
		public Int32 ShipMethodID { get; set; }
		public Int32? CreditCardID { get; set; }
		[StringLength(15)]
		public string CreditCardApprovalCode { get; set; }
		public Int32? CurrencyRateID { get; set; }
		public Decimal SubTotal { get; set; }
		public Decimal TaxAmt { get; set; }
		public Decimal Freight { get; set; }
		public Decimal TotalDue { get; set; }
		[StringLength(128)]
		public string Comment { get; set; }
		public Guid rowguid { get; set; }
		public DateTime ModifiedDate { get; set; }

		// We want to keep the details in the model
		public IList<SalesOrderDetail> OrderDetails { get; private set; }
	}

	public class SalesOrderDetail {
		public SalesOrderDetail() { }

		// Use reference instead
		// public Int32 SalesOrderID { get; set; }

		public Int32 SalesOrderDetailID { get; set; }
		[StringLength(25)]
		public string CarrierTrackingNumber { get; set; }
		public Int16 OrderQty { get; set; }
		public Int32 ProductID { get; set; }
		public Int32 SpecialOfferID { get; set; }
		public Decimal UnitPrice { get; set; }
		public Decimal UnitPriceDiscount { get; set; }
		public Decimal LineTotal { get; set; }
		public Guid rowguid { get; set; }
		public DateTime ModifiedDate { get; set; }

		// Reference the order header
		public SalesOrderHeader SalesOrder { get; set; }
	}

	public class SpecialOfferProduct {
		public SpecialOfferProduct() { }

		public Int32 SpecialOfferID { get; set; }
		public Int32 ProductID { get; set; }
		public Guid rowguid { get; set; }
		public DateTime ModifiedDate { get; set; }
	}

	public class CurrencyRate {
		public CurrencyRate() { }

		public Int32 CurrencyRateID { get; set; }
		public DateTime CurrencyRateDate { get; set; }
		[Required]
		[StringLength(3)]
		public string FromCurrencyCode { get; set; }
		[Required]
		[StringLength(3)]
		public string ToCurrencyCode { get; set; }
		public Decimal AverageRate { get; set; }
		public Decimal EndOfDayRate { get; set; }
		public DateTime ModifiedDate { get; set; }
	}

	#endregion Models

	#region Repositories

	public class CurrencyRepository : IRepository<Currency, string> {
		public CurrencyRepository(IConnection connection) {
			Connection = connection;
		}

		public IConnection Connection { get; private set; }

		#region "IRepository"

		public IEnumerable<Currency> LoadAll() {
			return Connection.ExecuteReader<Currency>(new CurrencySelectAll());
		}

		public Currency Load(string key) {
			return Connection.Execute<Currency>(new CurrencySelect { CurrencyCode = key });
		}

		public int Save(Currency instance) {
			if (string.IsNullOrEmpty(instance.CurrencyCode))
				return Connection.MapExecute(new CurrencyInsert(), instance);
			else
				return Connection.MapExecute(new CurrencyUpdate(), instance);
		}

		public int Delete(string key) {
			return Connection.Execute(new CurrencyDelete { CurrencyCode = key });
		}

		#endregion "IRepository"

		#region "Commands"

		//-------------------------------------------------------
		// SelectAll Command
		//-------------------------------------------------------
		public class CurrencySelectAll : ICommand {
			public CurrencySelectAll() { }

			public string GetScript() {
				return
	@"SELECT
[CurrencyCode]
,  [Name]
,  [ModifiedDate]
FROM
[Sales].[Currency]
";
			}
		}

		//-------------------------------------------------------
		// -- Select Command
		//-------------------------------------------------------
		public class CurrencySelect : ICommand {
			public CurrencySelect() { }

			public string GetScript() {
				return
	@"SELECT
[CurrencyCode]
,  [Name]
,  [ModifiedDate]
FROM
[Sales].[Currency]
WHERE
[CurrencyCode] = @CurrencyCode
";
			}

			[Parameter(3)]
			public string CurrencyCode { get; set; }
		}

		//-------------------------------------------------------
		// Insert Command
		//-------------------------------------------------------
		public class CurrencyInsert : ICommand {
			public CurrencyInsert() { }

			public string GetScript() {
				return
	@"INSERT INTO [Sales].[Currency] (
[CurrencyCode]
,  [Name]
,  [ModifiedDate]
)
VALUES (
@CurrencyCode
,  @Name
,  @ModifiedDate
)
";
			}

			[Parameter(3)]
			public string CurrencyCode { get; set; }
			[Parameter(50)]
			public string Name { get; set; }
			public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Update Command
		//-------------------------------------------------------
		public class CurrencyUpdate : ICommand {
			public CurrencyUpdate() { }

			public string GetScript() {
				return
	@"UPDATE
[Sales].[Currency]
SET
[Name] = @Name
,  [ModifiedDate] = @ModifiedDate
WHERE
[CurrencyCode] = @CurrencyCode
";
			}

			[Parameter(3)]
			public string CurrencyCode { get; set; }
			[Parameter(50)]
			public string Name { get; set; }
			public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Delete Command
		//-------------------------------------------------------
		public class CurrencyDelete : ICommand {
			public CurrencyDelete() { }

			public string GetScript() {
				return
	@"DELETE FROM
[Sales].[Currency]
WHERE
[CurrencyCode] = @CurrencyCode
";
			}

			[Parameter(3)]
			public string CurrencyCode { get; set; }
		}
		#endregion "Commands"
	}

	public class ShipMethodRepository : IRepository<ShipMethod, int> {
		public ShipMethodRepository(IConnection connection) {
			Connection = connection;
		}

		public IConnection Connection { get; private set; }

		#region "IRepository"

		public IEnumerable<ShipMethod> LoadAll() {
			return Connection.ExecuteReader<ShipMethod>(new ShipMethodSelectAll());
		}

		public ShipMethod Load(int key) {
			return Connection.Execute<ShipMethod>(new ShipMethodSelect { ShipMethodID = key });
		}

		public int Save(ShipMethod instance) {
			if (instance.ShipMethodID == 0)
				return Connection.MapExecute(new ShipMethodInsert(), instance);
			else
				return Connection.MapExecute(new ShipMethodUpdate(), instance);
		}

		public int Delete(int key) {
			return Connection.Execute(new ShipMethodDelete { ShipMethodID = key });
		}

		#endregion "IRepository"

		#region "Commands"

		//-------------------------------------------------------
		// SelectAll Command
		//-------------------------------------------------------
		public class ShipMethodSelectAll : ICommand {
			public ShipMethodSelectAll() { }

			public string GetScript() {
				return
	@"SELECT
[ShipMethodID]
,  [Name]
,  [ShipBase]
,  [ShipRate]
,  [rowguid]
,  [ModifiedDate]
FROM
[Purchasing].[ShipMethod]
";
			}
		}

		//-------------------------------------------------------
		// -- Select Command
		//-------------------------------------------------------
		public class ShipMethodSelect : ICommand {
			public ShipMethodSelect() { }

			public string GetScript() {
				return
	@"SELECT
[ShipMethodID]
,  [Name]
,  [ShipBase]
,  [ShipRate]
,  [rowguid]
,  [ModifiedDate]
FROM
[Purchasing].[ShipMethod]
WHERE
[ShipMethodID] = @ShipMethodID
";
			}

			public Int32 ShipMethodID { get; set; }
		}

		//-------------------------------------------------------
		// Insert Command
		//-------------------------------------------------------
		public class ShipMethodInsert : ICommand {
			public ShipMethodInsert() { }

			public string GetScript() {
				return
	@"INSERT INTO [Purchasing].[ShipMethod] (
[Name]
,  [ShipBase]
,  [ShipRate]
,  [rowguid]
,  [ModifiedDate]
)
VALUES (
@Name
,  @ShipBase
,  @ShipRate
,  @rowguid
,  @ModifiedDate
)

SET @ShipMethodID = SCOPE_IDENTITY();
";
			}

			[Output]
			public Int32 ShipMethodID { get; set; }
			[Parameter(50)]
			public string Name { get; set; }
			[Parameter(19, 4)]
			public Decimal ShipBase { get; set; }
			[Parameter(19, 4)]
			public Decimal ShipRate { get; set; }
			public Guid rowguid { get; set; }
			public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Update Command
		//-------------------------------------------------------
		public class ShipMethodUpdate : ICommand {
			public ShipMethodUpdate() { }

			public string GetScript() {
				return
	@"UPDATE
[Purchasing].[ShipMethod]
SET
[Name] = @Name
,  [ShipBase] = @ShipBase
,  [ShipRate] = @ShipRate
,  [rowguid] = @rowguid
,  [ModifiedDate] = @ModifiedDate
WHERE
[ShipMethodID] = @ShipMethodID
";
			}

			public Int32 ShipMethodID { get; set; }
			[Parameter(50)]
			public string Name { get; set; }
			[Parameter(19, 4)]
			public Decimal ShipBase { get; set; }
			[Parameter(19, 4)]
			public Decimal ShipRate { get; set; }
			public Guid rowguid { get; set; }
			public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Delete Command
		//-------------------------------------------------------
		public class ShipMethodDelete : ICommand {
			public ShipMethodDelete() { }

			public string GetScript() {
				return
	@"DELETE FROM
[Purchasing].[ShipMethod]
WHERE
[ShipMethodID] = @ShipMethodID
";
			}

			public Int32 ShipMethodID { get; set; }
		}

		#endregion "Commands"
	}

	public class SalesOrderHeaderRepository : IRepository<SalesOrderHeader, int> {
		public SalesOrderHeaderRepository(IConnection connection) {
			Connection = connection;
		}

		public IConnection Connection { get; private set; }

		#region "IRepository"

		public IEnumerable<SalesOrderHeader> LoadAll() {
			return Connection.ExecuteReader<SalesOrderHeader>(new SalesOrderHeaderSelectAll());
		}

		public SalesOrderHeader Load(int key) {
			return Connection.Execute<SalesOrderHeader>(new SalesOrderHeaderSelect { SalesOrderID = key });
		}

		public int Save(SalesOrderHeader instance) {
			if (instance.SalesOrderID == 0)
			    return Connection.MapExecute(new SalesOrderHeaderInsert(), instance);
			else
			    return Connection.MapExecute(new SalesOrderHeaderUpdate(), instance);
		}

		public int Delete(int key) {
			return Connection.Execute(new SalesOrderHeaderDelete { SalesOrderID = key });
		}

		#endregion "IRepository"

		#region "Commands"

		//-------------------------------------------------------
		// SelectAll Command
		//-------------------------------------------------------
		public class SalesOrderHeaderSelectAll : ICommand {
			public SalesOrderHeaderSelectAll() { }

			public string GetScript() {
				return
	@"SELECT
[SalesOrderID]
,  [RevisionNumber]
,  [OrderDate]
,  [DueDate]
,  [ShipDate]
,  [Status]
,  [OnlineOrderFlag]
,  [SalesOrderNumber]
,  [PurchaseOrderNumber]
,  [AccountNumber]
,  [CustomerID]
,  [SalesPersonID]
,  [TerritoryID]
,  [BillToAddressID]
,  [ShipToAddressID]
,  [ShipMethodID]
,  [CreditCardID]
,  [CreditCardApprovalCode]
,  [CurrencyRateID]
,  [SubTotal]
,  [TaxAmt]
,  [Freight]
,  [TotalDue]
,  [Comment]
,  [rowguid]
,  [ModifiedDate]
FROM
[Sales].[SalesOrderHeader]
";
			}
		}

		//-------------------------------------------------------
		// -- Select Command
		//-------------------------------------------------------
		public class SalesOrderHeaderSelect : ICommand {
			public SalesOrderHeaderSelect() { }

			public string GetScript() {
				return
	@"SELECT
[SalesOrderID]
,  [RevisionNumber]
,  [OrderDate]
,  [DueDate]
,  [ShipDate]
,  [Status]
,  [OnlineOrderFlag]
,  [SalesOrderNumber]
,  [PurchaseOrderNumber]
,  [AccountNumber]
,  [CustomerID]
,  [SalesPersonID]
,  [TerritoryID]
,  [BillToAddressID]
,  [ShipToAddressID]
,  [ShipMethodID]
,  [CreditCardID]
,  [CreditCardApprovalCode]
,  [CurrencyRateID]
,  [SubTotal]
,  [TaxAmt]
,  [Freight]
,  [TotalDue]
,  [Comment]
,  [rowguid]
,  [ModifiedDate]
FROM
[Sales].[SalesOrderHeader]
WHERE
[SalesOrderID] = @SalesOrderID
";
			}

			public Int32 SalesOrderID { get; set; }
		}

		//-------------------------------------------------------
		// Insert Command
		//-------------------------------------------------------
		public class SalesOrderHeaderInsert : ICommand {
			public SalesOrderHeaderInsert() { }

			public string GetScript() {
				return
	@"INSERT INTO [Sales].[SalesOrderHeader] (
[RevisionNumber]
,  [OrderDate]
,  [DueDate]
,  [ShipDate]
,  [Status]
,  [OnlineOrderFlag]
--,  [SalesOrderNumber]
,  [PurchaseOrderNumber]
,  [AccountNumber]
,  [CustomerID]
,  [SalesPersonID]
,  [TerritoryID]
,  [BillToAddressID]
,  [ShipToAddressID]
,  [ShipMethodID]
,  [CreditCardID]
,  [CreditCardApprovalCode]
,  [CurrencyRateID]
,  [SubTotal]
,  [TaxAmt]
,  [Freight]
--,  [TotalDue]
,  [Comment]
,  [rowguid]
,  [ModifiedDate]
)
VALUES (
@RevisionNumber
,  @OrderDate
,  @DueDate
,  @ShipDate
,  @Status
,  @OnlineOrderFlag
--,  @SalesOrderNumber
,  @PurchaseOrderNumber
,  @AccountNumber
,  @CustomerID
,  @SalesPersonID
,  @TerritoryID
,  @BillToAddressID
,  @ShipToAddressID
,  @ShipMethodID
,  @CreditCardID
,  @CreditCardApprovalCode
,  @CurrencyRateID
,  @SubTotal
,  @TaxAmt
,  @Freight
--,  @TotalDue
,  @Comment
,  newid()
,  @ModifiedDate
)

SET @SalesOrderID = SCOPE_IDENTITY();
";
			}

			[Output]
			public Int32 SalesOrderID { get; set; }

			[Input] 
            public Byte RevisionNumber { get; set; }
            
            [Input]
            public DateTime OrderDate { get; set; }
            
            [Input]
            public DateTime DueDate { get; set; }
            
            [Input]
            public DateTime? ShipDate { get; set; }
            
            [Input]
            public Byte Status { get; set; }
            
            [Input]
            public Boolean OnlineOrderFlag { get; set; }
            
            [Input]
            [Parameter(25)]
			public string SalesOrderNumber { get; set; }
            
            [Input]
            [Parameter(25, true)]
			public string PurchaseOrderNumber { get; set; }
            
            [Input]
            [Parameter(15, true)]
            public string AccountNumber { get; set; }
            
            [Input]
            public Int32 CustomerID { get; set; }
            
            [Input]
            public Int32? SalesPersonID { get; set; }
            
            [Input]
            public Int32? TerritoryID { get; set; }
            
            [Input]
            public Int32 BillToAddressID { get; set; }
            
            [Input]
            public Int32 ShipToAddressID { get; set; }
            
            [Input]
            public Int32 ShipMethodID { get; set; }
            
            [Input]
            public Int32? CreditCardID { get; set; }
            
            [Input]
            [Parameter(15, true)]
			public string CreditCardApprovalCode { get; set; }
            
            [Input]
            public Int32? CurrencyRateID { get; set; }
			
            [Parameter(19, 4)]
            [Input]
            public Decimal SubTotal { get; set; }
			
            [Parameter(19, 4)]
            [Input]
            public Decimal TaxAmt { get; set; }
			
            [Parameter(19, 4)]
            [Input]
            public Decimal Freight { get; set; }
            
            [Input]
            [Parameter(19, 4)]
			public Decimal TotalDue { get; set; }
            
            [Input]
            [Parameter(128, true)]
			public string Comment { get; set; }
			
            public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Update Command
		//-------------------------------------------------------
		public class SalesOrderHeaderUpdate : ICommand {
			public SalesOrderHeaderUpdate() { }

			public string GetScript() {
				return
	@"UPDATE
[Sales].[SalesOrderHeader]
SET
[RevisionNumber] = @RevisionNumber
,  [OrderDate] = @OrderDate
,  [DueDate] = @DueDate
,  [ShipDate] = @ShipDate
,  [Status] = @Status
,  [OnlineOrderFlag] = @OnlineOrderFlag
--,  [SalesOrderNumber] = @SalesOrderNumber
,  [PurchaseOrderNumber] = @PurchaseOrderNumber
,  [AccountNumber] = @AccountNumber
,  [CustomerID] = @CustomerID
,  [SalesPersonID] = @SalesPersonID
,  [TerritoryID] = @TerritoryID
,  [BillToAddressID] = @BillToAddressID
,  [ShipToAddressID] = @ShipToAddressID
,  [ShipMethodID] = @ShipMethodID
,  [CreditCardID] = @CreditCardID
,  [CreditCardApprovalCode] = @CreditCardApprovalCode
,  [CurrencyRateID] = @CurrencyRateID
--,  [SubTotal] = @SubTotal
,  [TaxAmt] = @TaxAmt
,  [Freight] = @Freight
--,  [TotalDue] = @TotalDue
,  [Comment] = @Comment
--,  [rowguid] = @rowguid
,  [ModifiedDate] = @ModifiedDate
WHERE
[SalesOrderID] = @SalesOrderID
";
			}

			public Int32 SalesOrderID { get; set; }
			public Byte RevisionNumber { get; set; }
			public DateTime OrderDate { get; set; }
			public DateTime DueDate { get; set; }
			public DateTime? ShipDate { get; set; }
			public Byte Status { get; set; }
			public Boolean OnlineOrderFlag { get; set; }
			[Parameter(25)]
			public string SalesOrderNumber { get; set; }
			[Parameter(25, true)]
			public string PurchaseOrderNumber { get; set; }
			[Parameter(15, true)]
			public string AccountNumber { get; set; }
			public Int32 CustomerID { get; set; }
			public Int32? SalesPersonID { get; set; }
			public Int32? TerritoryID { get; set; }
			public Int32 BillToAddressID { get; set; }
			public Int32 ShipToAddressID { get; set; }
			public Int32 ShipMethodID { get; set; }
			public Int32? CreditCardID { get; set; }
			[Parameter(15, true)]
			public string CreditCardApprovalCode { get; set; }
			public Int32? CurrencyRateID { get; set; }
			[Parameter(19, 4)]
			public Decimal SubTotal { get; set; }
			[Parameter(19, 4)]
			public Decimal TaxAmt { get; set; }
			[Parameter(19, 4)]
			public Decimal Freight { get; set; }
			[Parameter(19, 4)]
			public Decimal TotalDue { get; set; }
			[Parameter(128, true)]
			public string Comment { get; set; }
			public Guid rowguid { get; set; }
			public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Delete Command
		//-------------------------------------------------------
		public class SalesOrderHeaderDelete : ICommand {
			public SalesOrderHeaderDelete() { }

			public string GetScript() {
				return
	@"DELETE FROM
[Sales].[SalesOrderHeader]
WHERE
[SalesOrderID] = @SalesOrderID
";
			}

			public Int32 SalesOrderID { get; set; }
		}
		#endregion "Commands"
	}

	public class SalesOrderDetailRepository : IRepository<SalesOrderDetail, int> {
		public SalesOrderDetailRepository(IConnection connection) {
			Connection = connection;
		}

		public IConnection Connection { get; private set; }

		#region "IRepository"

		public IEnumerable<SalesOrderDetail> LoadAll() {
			return Connection.ExecuteReader<SalesOrderDetail>(new SalesOrderDetailSelectAll());
		}

		public SalesOrderDetail Load(int key) {
			return Connection.Execute<SalesOrderDetail>(new SalesOrderDetailSelect { SalesOrderDetailID = key });
		}

		public int Save(SalesOrderDetail instance) {
			if (instance.SalesOrderDetailID == 0)
			    return Connection.MapExecute(new SalesOrderDetailInsert(), instance);
			else
			    return Connection.MapExecute(new SalesOrderDetailUpdate(), instance);
		}

		public int Delete(int key) {
			return Connection.Execute(new SalesOrderDetailDelete { SalesOrderDetailID = key });
		}

		#endregion "IRepository"

		#region "Commands"

		//-------------------------------------------------------
		// SelectAll Command
		//-------------------------------------------------------
		public class SalesOrderDetailSelectAll : ICommand {
			public SalesOrderDetailSelectAll() { }

			public string GetScript() {
				return
	@"SELECT
[SalesOrderID]
,  [SalesOrderDetailID]
,  [CarrierTrackingNumber]
,  [OrderQty]
,  [ProductID]
,  [SpecialOfferID]
,  [UnitPrice]
,  [UnitPriceDiscount]
,  [LineTotal]
,  [rowguid]
,  [ModifiedDate]
FROM
[Sales].[SalesOrderDetail]
";
			}
		}

		//-------------------------------------------------------
		// -- Select Command
		//-------------------------------------------------------
		public class SalesOrderDetailSelect : ICommand {
			public SalesOrderDetailSelect() { }

			public string GetScript() {
				return
	@"SELECT
[SalesOrderID]
,  [SalesOrderDetailID]
,  [CarrierTrackingNumber]
,  [OrderQty]
,  [ProductID]
,  [SpecialOfferID]
,  [UnitPrice]
,  [UnitPriceDiscount]
,  [LineTotal]
,  [rowguid]
,  [ModifiedDate]
FROM
[Sales].[SalesOrderDetail]
WHERE
[SalesOrderID] = @SalesOrderID
AND [SalesOrderDetailID] = @SalesOrderDetailID
";
			}

			public Int32 SalesOrderID { get; set; }
			public Int32 SalesOrderDetailID { get; set; }
		}

		//-------------------------------------------------------
		// Insert Command
		//-------------------------------------------------------
		public class SalesOrderDetailInsert : ICommand {
			public SalesOrderDetailInsert() { }

			public string GetScript() {
				return
	@"INSERT INTO [Sales].[SalesOrderDetail] (
[SalesOrderID]
,  [CarrierTrackingNumber]
,  [OrderQty]
,  [ProductID]
,  [SpecialOfferID]
,  [UnitPrice]
,  [UnitPriceDiscount]
--,  [LineTotal]
,  [rowguid]
,  [ModifiedDate]
)
VALUES (
@SalesOrderID
,  @CarrierTrackingNumber
,  @OrderQty
,  @ProductID
,  @SpecialOfferID
,  @UnitPrice
,  @UnitPriceDiscount
--,  @LineTotal
,  newid()
,  @ModifiedDate
)

SET @SalesOrderDetailID = SCOPE_IDENTITY();
";
			}

			public Int32 SalesOrderID { get; set; }
			[Output]
			public Int32 SalesOrderDetailID { get; set; }
			[Parameter(25, true)]
			public string CarrierTrackingNumber { get; set; }
			public Int16 OrderQty { get; set; }
			public Int32 ProductID { get; set; }
			public Int32 SpecialOfferID { get; set; }
			[Parameter(19, 4)]
			public Decimal UnitPrice { get; set; }
			[Parameter(19, 4)]
			public Decimal UnitPriceDiscount { get; set; }
			[Parameter(38, 6)]
			public Decimal LineTotal { get; set; }
			public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Update Command
		//-------------------------------------------------------
		public class SalesOrderDetailUpdate : ICommand {
			public SalesOrderDetailUpdate() { }

			public string GetScript() {
				return
	@"UPDATE
[Sales].[SalesOrderDetail]
SET
[CarrierTrackingNumber] = @CarrierTrackingNumber
,  [OrderQty] = @OrderQty
,  [ProductID] = @ProductID
,  [SpecialOfferID] = @SpecialOfferID
,  [UnitPrice] = @UnitPrice
,  [UnitPriceDiscount] = @UnitPriceDiscount
--,  [LineTotal] = @LineTotal
--,  [rowguid] = @rowguid
,  [ModifiedDate] = @ModifiedDate
WHERE
[SalesOrderID] = @SalesOrderID
AND [SalesOrderDetailID] = @SalesOrderDetailID
";
			}

			public Int32 SalesOrderID { get; set; }
			public Int32 SalesOrderDetailID { get; set; }
			[Parameter(25, true)]
			public string CarrierTrackingNumber { get; set; }
			public Int16 OrderQty { get; set; }
			public Int32 ProductID { get; set; }
			public Int32 SpecialOfferID { get; set; }
			[Parameter(19, 4)]
			public Decimal UnitPrice { get; set; }
			[Parameter(19, 4)]
			public Decimal UnitPriceDiscount { get; set; }
			[Parameter(38, 6)]
			public Decimal LineTotal { get; set; }
			public Guid rowguid { get; set; }
			public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Delete Command
		//-------------------------------------------------------
		public class SalesOrderDetailDelete : ICommand {
			public SalesOrderDetailDelete() { }

			public string GetScript() {
				return
	@"DELETE FROM
[Sales].[SalesOrderDetail]
WHERE
[SalesOrderID] = @SalesOrderID
AND [SalesOrderDetailID] = @SalesOrderDetailID
";
			}

			public Int32 SalesOrderID { get; set; }
			public Int32 SalesOrderDetailID { get; set; }
		}
		#endregion "Commands"
	}

	public class SpecialOfferProductRepository : IRepository<SpecialOfferProduct, SpecialOfferProduct> {
		public SpecialOfferProductRepository(IConnection connection) {
			Connection = connection;
		}

		public IConnection Connection { get; private set; }

		#region "IRepository"

		public IEnumerable<SpecialOfferProduct> LoadAll() {
			return Connection.ExecuteReader<SpecialOfferProduct>(new SpecialOfferProductSelectAll());
		}

		public SpecialOfferProduct Load(SpecialOfferProduct key) {
			var cmd = new SpecialOfferProductSelect {
				SpecialOfferID = key.SpecialOfferID, ProductID = key.ProductID
			};
			return Connection.Execute<SpecialOfferProduct>(cmd);
		}

		public int Save(SpecialOfferProduct instance) {
			if (instance.SpecialOfferID == 0 || instance.ProductID == 0)
			    return Connection.MapExecute(new SpecialOfferProductInsert(), instance);
			else
			    return Connection.MapExecute(new SpecialOfferProductUpdate(), instance);
		}

		public int Delete(SpecialOfferProduct key) {
			return Connection.MapExecute(new SpecialOfferProductDelete(), key);
		}

		#endregion "IRepository"

		#region "Commands"

		//-------------------------------------------------------
		// SelectAll Command
		//-------------------------------------------------------
		public class SpecialOfferProductSelectAll : ICommand {
			public SpecialOfferProductSelectAll() { }

			public string GetScript() {
				return
	@"SELECT
   [SpecialOfferID]
,  [ProductID]
,  [rowguid]
,  [ModifiedDate]
FROM
	[Sales].[SpecialOfferProduct]
";
			}
		}

		//-------------------------------------------------------
		// -- Select Command
		//-------------------------------------------------------
		public class SpecialOfferProductSelect : ICommand {
			public SpecialOfferProductSelect() { }

			public string GetScript() {
				return
	@"SELECT
   [SpecialOfferID]
,  [ProductID]
,  [rowguid]
,  [ModifiedDate]
FROM
	[Sales].[SpecialOfferProduct]
WHERE
	[SpecialOfferID] = @SpecialOfferID
	AND [ProductID] = @ProductID
";
			}

			public Int32 SpecialOfferID { get; set; }
			public Int32 ProductID { get; set; }
		}

		//-------------------------------------------------------
		// Insert Command
		//-------------------------------------------------------
		public class SpecialOfferProductInsert : ICommand {
			public SpecialOfferProductInsert() { }

			public string GetScript() {
				return
	@"INSERT INTO [Sales].[SpecialOfferProduct] (
   [SpecialOfferID]
,  [ProductID]
,  [rowguid]
,  [ModifiedDate]
)
VALUES (
   @SpecialOfferID
,  @ProductID
,  @rowguid
,  @ModifiedDate
)
";
			}

			public Int32 SpecialOfferID { get; set; }
			public Int32 ProductID { get; set; }
			public Guid rowguid { get; set; }
			public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Update Command
		//-------------------------------------------------------
		public class SpecialOfferProductUpdate : ICommand {
			public SpecialOfferProductUpdate() { }

			public string GetScript() {
				return
	@"UPDATE
	[Sales].[SpecialOfferProduct]
SET
   [rowguid] = @rowguid
,  [ModifiedDate] = @ModifiedDate
WHERE
	[SpecialOfferID] = @SpecialOfferID
	AND [ProductID] = @ProductID
";
			}

			public Int32 SpecialOfferID { get; set; }
			public Int32 ProductID { get; set; }
			public Guid rowguid { get; set; }
			public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Delete Command
		//-------------------------------------------------------
		public class SpecialOfferProductDelete : ICommand {
			public SpecialOfferProductDelete() { }

			public string GetScript() {
				return
	@"DELETE FROM
	[Sales].[SpecialOfferProduct]
WHERE
	[SpecialOfferID] = @SpecialOfferID
	AND [ProductID] = @ProductID
";
			}

			public Int32 SpecialOfferID { get; set; }
			public Int32 ProductID { get; set; }
		}

		#endregion "Commands"
	}

	public class CurrencyRateRepository : IRepository<CurrencyRate, int> {
		public CurrencyRateRepository(IConnection connection) {
			Connection = connection;
		}

		public IConnection Connection { get; private set; }

		#region "IRepository"

		public IEnumerable<CurrencyRate> LoadAll() {
			return Connection.ExecuteReader<CurrencyRate>(new CurrencyRateSelectAll());
		}

		public CurrencyRate Load(int key) {
			return Connection.Execute<CurrencyRate>(new CurrencyRateSelect { CurrencyRateID = key });
		}

		public int Save(CurrencyRate instance) {
			if (instance.CurrencyRateID == 0)
			    return Connection.MapExecute(new CurrencyRateInsert(), instance);
			else
			    return Connection.MapExecute(new CurrencyRateUpdate(), instance);
		}

		public int Delete(int key) {
			return Connection.Execute(new CurrencyRateDelete { CurrencyRateID = key });
		}

		#endregion "IRepository"

		#region "Commands"

		//-------------------------------------------------------
		// SelectAll Command
		//-------------------------------------------------------
		public class CurrencyRateSelectAll : ICommand {
			public CurrencyRateSelectAll() { }

			public string GetScript() {
				return
	@"SELECT
   [CurrencyRateID]
,  [CurrencyRateDate]
,  [FromCurrencyCode]
,  [ToCurrencyCode]
,  [AverageRate]
,  [EndOfDayRate]
,  [ModifiedDate]
FROM
	[Sales].[CurrencyRate]
";
			}
		}

		//-------------------------------------------------------
		// -- Select Command
		//-------------------------------------------------------
		public class CurrencyRateSelect : ICommand {
			public CurrencyRateSelect() { }

			public string GetScript() {
				return
	@"SELECT
   [CurrencyRateID]
,  [CurrencyRateDate]
,  [FromCurrencyCode]
,  [ToCurrencyCode]
,  [AverageRate]
,  [EndOfDayRate]
,  [ModifiedDate]
FROM
	[Sales].[CurrencyRate]
WHERE
	[CurrencyRateID] = @CurrencyRateID
";
			}

			public Int32 CurrencyRateID { get; set; }
		}

		//-------------------------------------------------------
		// Insert Command
		//-------------------------------------------------------
		public class CurrencyRateInsert : ICommand {
			public CurrencyRateInsert() { }

			public string GetScript() {
				return
	@"INSERT INTO [Sales].[CurrencyRate] (
   [CurrencyRateDate]
,  [FromCurrencyCode]
,  [ToCurrencyCode]
,  [AverageRate]
,  [EndOfDayRate]
,  [ModifiedDate]
)
VALUES (
   @CurrencyRateDate
,  @FromCurrencyCode
,  @ToCurrencyCode
,  @AverageRate
,  @EndOfDayRate
,  @ModifiedDate
)

SET @CurrencyRateID = SCOPE_IDENTITY();
";
			}

			[Output]
			public Int32 CurrencyRateID { get; set; }
			public DateTime CurrencyRateDate { get; set; }
			[Parameter(3)]
			public string FromCurrencyCode { get; set; }
			[Parameter(3)]
			public string ToCurrencyCode { get; set; }
			[Parameter(19, 4)]
			public Decimal AverageRate { get; set; }
			[Parameter(19, 4)]
			public Decimal EndOfDayRate { get; set; }
			public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Update Command
		//-------------------------------------------------------
		public class CurrencyRateUpdate : ICommand {
			public CurrencyRateUpdate() { }

			public string GetScript() {
				return
	@"UPDATE
	[Sales].[CurrencyRate]
SET
   [CurrencyRateDate] = @CurrencyRateDate
,  [FromCurrencyCode] = @FromCurrencyCode
,  [ToCurrencyCode] = @ToCurrencyCode
,  [AverageRate] = @AverageRate
,  [EndOfDayRate] = @EndOfDayRate
,  [ModifiedDate] = @ModifiedDate
WHERE
	[CurrencyRateID] = @CurrencyRateID
";
			}

			public Int32 CurrencyRateID { get; set; }
			public DateTime CurrencyRateDate { get; set; }
			[Parameter(3)]
			public string FromCurrencyCode { get; set; }
			[Parameter(3)]
			public string ToCurrencyCode { get; set; }
			[Parameter(19, 4)]
			public Decimal AverageRate { get; set; }
			[Parameter(19, 4)]
			public Decimal EndOfDayRate { get; set; }
			public DateTime ModifiedDate { get; set; }
		}

		//-------------------------------------------------------
		// Delete Command
		//-------------------------------------------------------
		public class CurrencyRateDelete : ICommand {
			public CurrencyRateDelete() { }

			public string GetScript() {
				return
	@"DELETE FROM
	[Sales].[CurrencyRate]
WHERE
	[CurrencyRateID] = @CurrencyRateID
";
			}

			public Int32 CurrencyRateID { get; set; }
		}

		#endregion "Commands"
	}

	#endregion Repositories

	public static class SqlCeConnector {
		public const string ConnectionString = @"Data Source=|DataDirectory|\Northwind40.sdf";

		public static IDbConnection GetConnection() {
			return new System.Data.SqlServerCe.SqlCeConnection(ConnectionString);
		}

		public static IDataRecord GetFirstEmployeeRecord(IDbConnection connection) {
			string query = @"
SELECT [Employee ID] AS EmployeeID, [First Name] AS FirstName, [Last Name] AS LastName, Title, [Birth Date] AS BirthDate, [Hire Date] AS HireDate, cast(NULL as nvarchar(1)) as Sex
FROM Employees AS E
WHERE ([Employee ID] = 1)";

			IDbCommand cmd = new System.Data.SqlServerCe.SqlCeCommand(query);
			cmd.Connection = connection;
			connection.Open();
			var dr = cmd.ExecuteReader(CommandBehavior.Default);
			dr.Read();
			return dr;
		}
	}

	[TestClass]
	public class MapperDataTests {
		static private IDbProvider dbprov = new SqlClientDbProvider();

		[TestMethod]
		public void TestDataRecordDefaultMappings() {
			using (var cn = SqlCeConnector.GetConnection()) {
				var dr = SqlCeConnector.GetFirstEmployeeRecord(cn);
				new EmployeeInfo().Read<EmployeeInfo>(dr, 0); // to build mapper
				Logger.Log(CommandMapper.Of<EmployeeInfo>.To<EmployeeInfo>.GetMappings(0), CommandMapper.Of<EmployeeInfo>.To<EmployeeInfo>.GetMapperExpression(0));
				Assert.IsTrue(CommandMapper.Of<EmployeeInfo>.To<EmployeeInfo>.GetMappings(0).Count() == 6, "Expected 6 mappings");
			}
		}

		[TestMethod]
		public void TestDataRecordDefaultMapper() {
			EmployeeInfo e;
			using (var cn = SqlCeConnector.GetConnection()) {
				var dr = SqlCeConnector.GetFirstEmployeeRecord(cn);
				e = new EmployeeInfo().Read<EmployeeInfo>(dr, 0);
			}
			Assert.AreEqual("Nancy", e.FirstName);
			Assert.AreEqual("Davolio", e.LastName);
			Assert.AreEqual("Sales Representative", e.Title);
			Assert.AreEqual("12/8/1948 12:00:00 AM", e.BirthDate.ToString());
			Assert.AreEqual("3/29/1991 12:00:00 AM", e.HireDate.ToString());
			Assert.IsNull(e.Sex);
		}

		[TestMethod]
		public void TestDbCommandDefaultMappings() {
			Logger.Log(CommandMapper.Of<EmployeeInfo>.InMappings, CommandMapper.Of<EmployeeInfo>.GetInMapperExpression());
			Logger.Log(CommandMapper.Of<EmployeeInfo>.OutMappings, CommandMapper.Of<EmployeeInfo>.GetOutMapperExpression());

			Assert.IsTrue(CommandMapper.Of<EmployeeInfo>.InMappings.Count() == 11, "Expected 11 mappings");
			Assert.IsTrue(CommandMapper.Of<EmployeeInfo>.OutMappings.Count() == 1, "Expected 1 mappings");
		}

		[TestMethod]
		public void TestDbCommandDefaultMapper() {
			var e = EmployeeInfo.CreateSample1();
			var cmd = dbprov.DbCommand(e);

			Assert.IsTrue(cmd.Parameters.Count == 11, "Expected 11 parameters");
			Assert.IsTrue((cmd.Parameters["EmployeeID"] as IDataParameter).Direction == ParameterDirection.Output, "Employee ID is output");
			Assert.IsTrue((cmd.Parameters["FirstName"] as IDataParameter).Value.ToString() == "John", "FirstName is John");
			Assert.IsTrue((cmd.Parameters["LastName"] as IDataParameter).Value.ToString() == "Doe", "LastName is Doe");
			Assert.IsTrue((cmd.Parameters["Title"] as IDataParameter).Value.ToString() == "Missing Person", "Title is Missing Person");
			Assert.IsTrue(((DateTime)(cmd.Parameters["BirthDate"] as IDataParameter).Value).Year == 1980, "BirthDate year is 1980");
			Assert.IsTrue(((DateTime)(cmd.Parameters["HireDate"] as IDataParameter).Value).Year == 2000, "HireDate year is 2000");
			Assert.IsTrue((cmd.Parameters["Sex"] as IDataParameter).Value == DBNull.Value, "Sex is null");

			Assert.IsTrue((cmd.Parameters["FirstName"] as IDataParameter).IsNullable == false, "FirstName is not nullable");
			Assert.IsTrue((cmd.Parameters["FirstName"] as SqlParameter).Size == 10, "FirstName size is 10");
			Assert.IsTrue((cmd.Parameters["LastName"] as IDataParameter).IsNullable == false, "LastName is not nullable");
			Assert.IsTrue((cmd.Parameters["LastName"] as SqlParameter).Size == 20, "LastName size is 20");
			Assert.IsTrue((cmd.Parameters["Title"] as IDataParameter).IsNullable == true, "Title is nullable");
			Assert.IsTrue((cmd.Parameters["Title"] as SqlParameter).Size == 30, "Title size is 30");

			(cmd.Parameters["EmployeeID"] as IDataParameter).Value = 123;
			cmd.Output(e);

			Assert.IsTrue(e.EmployeeID == 123, "EmployeeID is 123");
		}
	}

	[TestClass]
	public class ReaderDataTests {
		public class ReadEmployees : ICommand {
			public string GetScript() {
				return @"
SELECT [Employee ID] AS EmployeeID, [First Name] AS FirstName, [Last Name] AS LastName, Title, [Birth Date] AS BirthDate, [Hire Date] AS HireDate, cast(NULL as nvarchar(1)) as Sex
FROM Employees AS E";
			}
		}

        //[TestMethod]
        //public void TestDataReader() {
        //    var conn = new SqlServerCeConnection(SqlCeConnector.ConnectionString);
        //    conn.TimeOut = 0; // SqlServerCe
        //    var employees = conn.ExecuteReader<EmployeeInfo>(new ReadEmployees()).ToList();
        //    Assert.IsTrue(employees.Count() == 15, "Read 15 employees");
        //}
	}

	[TestClass]
	public class AdventureWorks2008R2DataTests {
		static IConnection c;

        static AdventureWorks2008R2DataTests() {
            c = new SqlClientConnection(@"Data Source=localhost\SQLEXPRESS;Initial Catalog=AdventureWorks2008R2;Integrated Security=True");
            c.AutoClose = false;

			// Error event is invoked by the framework when DAL exceptions occur 
			HandleErrors();

			// BeforeExecute event is invoked by the framework before executing commands.
			TraceExecution();
		}

		[TestMethod]
		[TestCategory("Data")]
		public void ReadCurrency() {
			string code = "USD";
            string name = "US Dollar";

			// read using repository
			var cr = new CurrencyRepository(c);
			var currency = cr.Load(code);
			Console.WriteLine(string.Format("Currency: [{0}] {1} modified on {2}", currency.CurrencyCode, currency.Name, currency.ModifiedDate));
            Assert.IsTrue(currency.CurrencyCode == code, "Code is " + code);
            Assert.IsTrue(currency.Name == name, "Name is " + name);

			// read using custom materializer
			currency = c.Execute<Currency>(new CurrencyRepository.CurrencySelect { CurrencyCode = code }, ctx => {
				var r = ctx.Record;
				var ccy = new Currency();
				ccy.CurrencyCode = r.GetString(0) + " Custom";
				ccy.Name = r.GetString(1) + " Custom";
				ccy.ModifiedDate = r.GetDateTime(2);
				return ccy;
			});
			Console.WriteLine(string.Format("Currency: [{0}] {1} modified on {2}", currency.CurrencyCode, currency.Name, currency.ModifiedDate));
            Assert.IsTrue(currency.CurrencyCode == code + " Custom", "Code is " + code + " Custom");
            Assert.IsTrue(currency.Name == name + " Custom", "Name is " + name + " Custom");
		}

		[TestMethod]
		[TestCategory("Data")]
		public void ReadAllCurrencies() {
			// read using repository
			var cr = new CurrencyRepository(c);
			var currencies = cr.LoadAll();
			PrintCurrencies(currencies);
			Assert.IsTrue(currencies.Last().CurrencyCode == "ZWD", "Last Code is ZWD");
			Assert.IsTrue(currencies.Count() > 50, "More than 50 currencies");

			// read using custom materializer
			currencies = c.ExecuteReader<Currency>(new CurrencyRepository.CurrencySelectAll(), ctx => {
				var r = ctx.Record;
				var ccy = new Currency();
				ccy.CurrencyCode = r.GetString(0) + " Custom";
				ccy.Name = r.GetString(1) + " Custom";
				ccy.ModifiedDate = r.GetDateTime(2);
				return ccy;
			});
			PrintCurrencies(currencies);
			Assert.IsTrue(currencies.Last().CurrencyCode == "ZWD Custom", "Last Code is ZWD Custom");
			Assert.IsTrue(currencies.Count() > 50, "More than 50 currencies");
		}

		[TestMethod]
		[TestCategory("Data")]
		public void ReadDictionaryOfCurrencies() {
			// read using repository
			var cr = new CurrencyRepository(c);
			var currencies = cr.LoadAll().ToDictionary<Currency, string>(ccy => ccy.CurrencyCode);
			PrintCurrencies(currencies.Values);
			Assert.IsTrue(currencies.Last().Value.CurrencyCode == "ZWD", "Last Code is ZWD");
			Assert.IsTrue(currencies.Count() > 50, "More than 50 currencies");
		}

		[TestMethod]
		[TestCategory("Data")]
		public void LoadLookups() {
			var lookups = new Lookups();
			var script =
@"
SELECT [CurrencyCode], [Name], [ModifiedDate] FROM	[Sales].[Currency];
SELECT [ShipMethodID], [Name], [ShipBase], [ShipRate], [rowguid], [ModifiedDate] FROM [Purchasing].[ShipMethod];";

			// read using custom materializer for lookups
			c.ExecuteReader<Lookups>(script, callback: ctx => {
				var r = ctx.Record;
				if (ctx.ResultIndex == 1) {
					var ccy = new Currency();
					ccy.CurrencyCode = r.GetString(0) + " Custom";
					ccy.Name = r.GetString(1) + " Custom";
					ccy.ModifiedDate = r.GetDateTime(2);
					lookups.Currencies[ccy.CurrencyCode] = ccy;
				}
				else if (ctx.ResultIndex == 2) {
					var sm = new ShipMethod();
					sm.ShipMethodID = r.GetInt32(0);
					sm.Name = r.GetString(1) + " Custom";
					sm.ModifiedDate = r.GetDateTime(5);
					lookups.ShipMethods[sm.ShipMethodID] = sm;
				}
				return null;
			}).Count();
			PrintCurrencies(lookups.Currencies.Values);
			PrintShipMethods(lookups.ShipMethods.Values);
            Assert.IsTrue(lookups.Currencies.Last().Value.CurrencyCode == "ZWD Custom", "Last Code is ZWD Custom");
            Assert.IsTrue(lookups.Currencies.Count() > 50, "More than 50 currencies");
            Assert.IsTrue(lookups.ShipMethods.Last().Value.Name == "CARGO TRANSPORT 5 Custom", "Last ShipMethod is CARGO TRANSPORT 5 Custom");
            Assert.IsTrue(lookups.ShipMethods.Count() == 5, "5 ShipMethods");

			// read using custom materializer for lookups and default materializers for internal types
			lookups = new Lookups();
			c.ExecuteReader<Lookups>(script, callback: ctx => {
				if (ctx.ResultIndex == 1) {
					var ccy = ctx.Materialize<Currency>();
					lookups.Currencies[ccy.CurrencyCode] = ccy;
				}
				else if (ctx.ResultIndex == 2) {
					var sm = ctx.Materialize<ShipMethod>();
					lookups.ShipMethods[sm.ShipMethodID] = sm;
				}
				return null;
			}).Count();
			PrintCurrencies(lookups.Currencies.Values);
			PrintShipMethods(lookups.ShipMethods.Values);
            Assert.IsTrue(lookups.Currencies.Last().Value.CurrencyCode == "ZWD", "Last Code is ZWD");
            Assert.IsTrue(lookups.Currencies.Count() > 50, "More than 50 currencies");
            Assert.IsTrue(lookups.ShipMethods.Last().Value.Name == "CARGO TRANSPORT 5", "Last ShipMethod is CARGO TRANSPORT 5");
            Assert.IsTrue(lookups.ShipMethods.Count() == 5, "5 ShipMethods");
        }

		[TestMethod]
		[TestCategory("Data")]
		public void GetLastModifiedCurrencyBefore5YearsAgo() {
			var code = GetLastModifiedCurrencyBefore(DateTime.Today.AddYears(-5));
            Assert.IsTrue(code.Length > 0, "Code returned");
		}

		[TestMethod]
		[TestCategory("Data")]
		public void GetLastModifiedCurrencyBeforeNow() {
			var code = GetLastModifiedCurrencyBefore(DateTime.Now);
            Assert.IsTrue(code.Length > 0, "Code returned");
		}

		[TestMethod]
		[TestCategory("Data")]
		public void InsertCurrenciesWithRate() {
			int records;

            try {
				// insert new currency ABC
				records = c.Execute(new CurrencyRepository.CurrencyInsert { CurrencyCode = "ABC", Name = "Test Currency ABC", ModifiedDate = DateTime.Now });
				Console.WriteLine(records + " inserted.");
			}
			catch (Exception ex) {
				Console.WriteLine(ex.Message);
			}

			try {
				// insert new currency 123
				records = c.Execute(new CurrencyRepository.CurrencyInsert { CurrencyCode = "123", Name = "Test Currency 123", ModifiedDate = DateTime.Now });
				Console.WriteLine(records + " inserted.");
			}
			catch (Exception ex) {
				Console.WriteLine(ex.Message);
			}

			// insert new rate from ABC to 123 and display new rate id
			var crt = new CurrencyRateRepository.CurrencyRateInsert {
				FromCurrencyCode = "ABC",
				ToCurrencyCode = "123",
				ModifiedDate = DateTime.Now,
				CurrencyRateDate = DateTime.Now,
				AverageRate = 1.5M,
				EndOfDayRate = 1.5M
			};
			records = c.Execute(crt);
			Console.WriteLine(string.Format("{0} inserted. New CurrencyRateID = {1}.", records, crt.CurrencyRateID));

            // read new currency rate
            var crr = new CurrencyRateRepository(c); 
            var cr = crr.Load(crt.CurrencyRateID);

            // cleanup
            crr.Delete(cr.CurrencyRateID);
            var crep = new CurrencyRepository(c);
            crep.Delete("ABC");
            crep.Delete("123");
        
            Assert.IsTrue(cr.FromCurrencyCode == "ABC", "From code is ABC");
            Assert.IsTrue(cr.ToCurrencyCode == "123", "To code is 123");
            Assert.IsTrue(cr.AverageRate == 1.5M, "Avg Rate is 1.5M");
        }

		[TestMethod]
		[TestCategory("Data")]
		public void InsertBoundCurrenciesWithRate() {
            CurrencyRate cr;

            var mi = Mapper.Of<CurrencyRate, CurrencyRateRepository.CurrencyRateInsert>.Mappings;
            var mo = Mapper.Of<CurrencyRateRepository.CurrencyRateInsert, CurrencyRate>.Mappings;

            Logger.Log(mi, null);
            Logger.Log(mo, null);
 
			// let's do this inside a transaction
			using (var ts = new TransactionScope()) {
				// insert new currency XYZ
				int records = c.MapExecute(
				    new CurrencyRepository.CurrencyInsert(),
				    new Currency { CurrencyCode = "XYZ", Name = "Test Currency XYZ", ModifiedDate = DateTime.Now }
				);
				Console.WriteLine(records + " inserted.");

				// create new currency rate from XYZ to USD
				cr = new CurrencyRate {
					FromCurrencyCode = "XYZ",
                    ToCurrencyCode = "USD",
					ModifiedDate = DateTime.Now,
					CurrencyRateDate = DateTime.Now,
					AverageRate = 1.2M,
					EndOfDayRate = 1.2M
				};

                // insert new rate (note the output parameter updating the property CurrencyRateID)
                records = c.MapExecute(new CurrencyRateRepository.CurrencyRateInsert(), cr);
                Console.WriteLine(string.Format("{0} inserted. New CurrencyRateID = {1}.", records, cr.CurrencyRateID));

				// commit changes
				ts.Complete();
			}

            // read new currency rate
            var crr = new CurrencyRateRepository(c);
            cr = crr.Load(cr.CurrencyRateID);
 
            // cleanup
            c.Execute("DELETE FROM [Sales].[CurrencyRate] WHERE 'XYZ' IN (FromCurrencyCode, ToCurrencyCode)");
            new CurrencyRepository(c).Delete("XYZ");

            Assert.IsTrue(cr.FromCurrencyCode == "XYZ", "From code is XYZ");
            Assert.IsTrue(cr.ToCurrencyCode == "USD", "To code is USD");
            Assert.IsTrue(cr.AverageRate == 1.2M, "Avg Rate is 1.2M");
        }

		[TestMethod]
		[TestCategory("Data")]
		public void InsertMultipleBoundCurrencies() {
			var currencies = new Currency[] {
				new Currency { CurrencyCode = "CC1", Name = "Test Currency CC1", ModifiedDate = DateTime.Now },
				new Currency { CurrencyCode = "CC2", Name = "Test Currency CC2", ModifiedDate = DateTime.Now },
				new Currency { CurrencyCode = "CC3", Name = "Test Currency CC3", ModifiedDate = DateTime.Now }
			};

			// let's do this inside a transaction
			using (var ts = new TransactionScope()) {
                // insert all currencies
                currencies.Select(cc => {
                    return c.MapExecute(new CurrencyRepository.CurrencyInsert(), cc);
                }).Count();

				// commit changes
				ts.Complete();
			}

            System.Threading.Thread.Sleep(10);
			var code = GetLastModifiedCurrencyBefore(DateTime.Now);

            // cleanup
            currencies.Select(cc => {
                return c.MapExecute(new CurrencyRepository.CurrencyDelete(), cc);
            }).Count();

            Assert.IsTrue(code.StartsWith("CC"), "Last code starts with CC");
        }

		[TestMethod]
		[TestCategory("Data")]
		public void InsertMultipleCurrencies() {
			var c1 = new CurrencyRepository.CurrencyDelete[] {
				new CurrencyRepository.CurrencyDelete { CurrencyCode = "CC4" },
				new CurrencyRepository.CurrencyDelete { CurrencyCode = "CC5" },
				new CurrencyRepository.CurrencyDelete { CurrencyCode = "CC6" }
			};

			var c2 = new CurrencyRepository.CurrencyInsert[] {
				new CurrencyRepository.CurrencyInsert { CurrencyCode = "CC4", Name = "Test Currency CC4", ModifiedDate = DateTime.Now },
				new CurrencyRepository.CurrencyInsert { CurrencyCode = "CC5", Name = "Test Currency CC5", ModifiedDate = DateTime.Now },
				new CurrencyRepository.CurrencyInsert { CurrencyCode = "CC6", Name = "Test Currency CC6", ModifiedDate = DateTime.Now }
			};

			// let's do this inside a transaction
			using (var ts = new TransactionScope()) {
				// insert all currencies
				c.Execute(c2);

				// commit changes
				ts.Complete();
			}

            System.Threading.Thread.Sleep(10);
            var code = GetLastModifiedCurrencyBefore(DateTime.Now);
            var count = c.ExecuteScalar<int>("select count(*) from [Sales].[Currency] where CurrencyCode like 'CC%'");

            // cleanup
            c.Execute(c1);

            Assert.IsTrue(code.StartsWith("CC"), "Last code starts with CC");
            Assert.IsTrue(count == 3, "3 CC currencies found");
		}

		[TestMethod]
		[TestCategory("Data")]
		public void DynamicReadCurrenciesBStart() {
			c.ExecutionMode = System.Data.CommandType.Text;
			var currencies = DynamicReadCurrencies("B%");
            var first = currencies.First();
            Assert.IsTrue(currencies.Count() > 9, "More than 9 currencies start with B");
            Assert.IsTrue(first.Name.ToString().StartsWith("B"), "Name starts with B");
            Assert.IsTrue(((Guid)first.AnythingElse).ToString().Length > 10, "There is a GUID");
		}

		[TestMethod]
		[TestCategory("Data")]
		public void DynamicReadCurrenciesRStart() {
			c.ExecutionMode = System.Data.CommandType.Text;
			var currencies = DynamicReadCurrencies("R%");
            var first = currencies.First();
            Assert.IsTrue(currencies.Count() > 3, "More than 3 currencies start with R");
            Assert.IsTrue(first.Name.ToString().StartsWith("R"), "Name starts with R");
            Assert.IsTrue(((Guid)first.AnythingElse).ToString().Length > 10, "There is a GUID");
        }

		[TestMethod]
		[TestCategory("Data")]
		public void ScriptNames() {
			var cmds = System.Reflection.Assembly.GetExecutingAssembly().GetCommands().ToList();
			cmds.ForEach(cmd => Console.WriteLine(c.GetName(cmd)));
			Assert.IsTrue(cmds.Count() == 32, "Expected 32 commands");
		}

        [TestMethod]
        [TestCategory("Data")]
        public void InsertSalesOrder() {
            // display mappings
            var mhi = Mapper.Of<SalesOrderHeader, SalesOrderHeaderRepository.SalesOrderHeaderInsert>.Mappings;
            var mho = Mapper.Of<SalesOrderHeaderRepository.SalesOrderHeaderInsert, SalesOrderHeader>.Mappings;
            var mdi = Mapper.Of<SalesOrderDetail, SalesOrderDetailRepository.SalesOrderDetailInsert>.Mappings;
            var mdo = Mapper.Of<SalesOrderDetailRepository.SalesOrderDetailInsert, SalesOrderDetail>.Mappings;

            Logger.Log(mhi, null);
            Logger.Log(mho, null);
            Logger.Log(mdi, null);
            Logger.Log(mdo, null);

            var order = CreateOrder(c);
            Assert.IsTrue(order.SalesOrderID == 0, "new order with orderid = 0");
            Assert.IsTrue(order.OrderDetails[0].SalesOrderDetailID == 0, "new order detail with id = 0");

            InsertSalesOrder(c, order);
            // cleanup
            DeleteSalesOrder(c, order);

            Assert.IsTrue(order.SalesOrderID > 0, "new order with orderid > 0");
            Assert.IsTrue(order.OrderDetails[0].SalesOrderDetailID > 0, "new order detail with id > 0");
            Assert.IsTrue(order.OrderDetails[1].SalesOrderDetailID > 0, "new order detail with id > 0");
        }

        [TestMethod]
        [TestCategory("Data")]
        public void InsertSalesOrderWithSp() {
            DeployCommands();
            
			var order = CreateOrder(c);
			Assert.IsTrue(order.SalesOrderID == 0, "new order with orderid = 0");
			Assert.IsTrue(order.OrderDetails[0].SalesOrderDetailID == 0, "new order detail with id = 0");
			
			c.ExecutionMode = System.Data.CommandType.StoredProcedure;
            InsertSalesOrder(c, order);
			c.ExecutionMode = CommandType.Text;
			// cleanup
            DeleteSalesOrder(c, order);

			Assert.IsTrue(order.SalesOrderID > 0, "new order with orderid > 0");
			Assert.IsTrue(order.OrderDetails[0].SalesOrderDetailID > 0, "new order detail with id > 0");
			Assert.IsTrue(order.OrderDetails[1].SalesOrderDetailID > 0, "new order detail with id > 0");
		}

		[TestMethod]
		[TestCategory("Data")]
		public void InsertSalesOrderInBatch() {
			DeployCommands();

			var order = CreateOrder(c);
			Assert.IsTrue(order.SalesOrderID == 0, "new order with orderid = 0");
			Assert.IsTrue(order.OrderDetails[0].SalesOrderDetailID == 0, "new order detail with id = 0");

			c.ExecutionMode = System.Data.CommandType.StoredProcedure;

            var batch = new Batch();
			var orderitem = batch.Add(new SalesOrderHeaderRepository.SalesOrderHeaderInsert(), order);
			foreach (var od in order.OrderDetails) {
				var detailitem = batch.Add(new SalesOrderDetailRepository.SalesOrderDetailInsert(), od);
                orderitem.BindTo(detailitem, s => s.SalesOrderID, t => t.SalesOrderID);
			}

            Console.WriteLine("-------------------------------------------------------------------------");
            Console.WriteLine(batch.GetScript(c));
            Console.WriteLine("-------------------------------------------------------------------------");

			using (var ts = new TransactionScope()) {
                batch.Execute(c);
			    ts.Complete();
			}
			Console.WriteLine(string.Format("Sales order inserted. SalesOrderId={0}, Line1Id= {1}, Line2Id={2}",
			    order.SalesOrderID, order.OrderDetails[0].SalesOrderDetailID, order.OrderDetails[1].SalesOrderDetailID));
	
			c.ExecutionMode = CommandType.Text;
			// cleanup
			DeleteSalesOrder(c, order);

			Assert.IsTrue(order.SalesOrderID > 0, "new order with orderid > 0");
			Assert.IsTrue(order.OrderDetails[0].SalesOrderDetailID > 0, "new order detail with id > 0");
			Assert.IsTrue(order.OrderDetails[1].SalesOrderDetailID > 0, "new order detail with id > 0");
		}

		#region "Helpers"

		static void HandleErrors() {
			c.AfterExecute += new EventHandler<CommandExecutionEventArgs>((s, a) => {
				if (a.Context.Error != null) {
					Console.WriteLine(string.Format("!!! Error executing command {0}. {1}.", a.Context.CommandName, a.Context.Error.Message));
					// don't rethrow to continue test
					a.IgnoreErrors = true;
				}
			});
		}

		static void TraceExecution() {
			c.BeforeExecute += new EventHandler<CommandExecutionEventArgs>((s, a) => {
				Console.WriteLine(string.Format("-> Executing command {0}.", a.Context.CommandName));
			});
		}

		static void DeployCommands() {
			var deployer = new Knak.Data.SqlServer.SqlServerCommandDeployer();
			deployer.Deploy(c,
				new SalesOrderHeaderRepository.SalesOrderHeaderInsert(),
				new SalesOrderDetailRepository.SalesOrderDetailInsert(),
				new SalesOrderHeaderRepository.SalesOrderHeaderDelete(),
				new SalesOrderDetailRepository.SalesOrderDetailDelete()
			);
		}

		static void PrintCurrencies(IEnumerable<Currency> currencies) {
			currencies.All(currency => {
				Console.WriteLine(string.Format("Currency: [{0}] {1} modified on {2}", currency.CurrencyCode, currency.Name, currency.ModifiedDate));
				return true;
			});
		}

		static void PrintShipMethods(IEnumerable<ShipMethod> shipMethods) {
			shipMethods.All(sm => {
				Console.WriteLine(string.Format("Ship Method: [{0}] {1} modified on {2}", sm.ShipMethodID, sm.Name, sm.ModifiedDate));
				return true;
			});
		}

		class Lookups {
			public Lookups() {
				Currencies = new Dictionary<string, Currency>();
				ShipMethods = new Dictionary<int, ShipMethod>();
			}

			public Dictionary<string, Currency> Currencies { get; private set; }
			public Dictionary<int, ShipMethod> ShipMethods { get; private set; }
		}

		static SalesOrderHeader CreateOrder(IConnection cn) {
			// -----------------------------------------------------------------------------------------------------
			// Get any customer's info
			var cinfoScript = 
@"
SELECT TOP 1
	C.[CustomerID]
,	C.[AccountNumber]   
,	A.[AddressID]
,	BA.[AddressTypeID]
FROM
	[Sales].[Customer] C
	join [Person].[Person] P on C.PersonID = P.BusinessEntityID
	join [Person].[BusinessEntityAddress] BA on P.BusinessEntityID = BA.BusinessEntityID 
	join [Person].[Address] A on BA.AddressID = A.AddressID 
";

            var cinfo = cn.Execute<dynamic>(cinfoScript, callback: DynamicReader.Materialize);

			// Find a ship method
			var shipmethod = new ShipMethodRepository(cn).LoadAll().First();
			// Load special offers
			var specialOffers = new SpecialOfferProductRepository(cn).LoadAll().ToList();
			// -----------------------------------------------------------------------------------------------------

			// create new order
			var order = new SalesOrderHeader {
                RevisionNumber = 0,
				CustomerID = cinfo.CustomerID,
				AccountNumber = cinfo.AccountNumber,
				BillToAddressID = cinfo.AddressID,
				ShipToAddressID = cinfo.AddressID,
				ShipMethodID = shipmethod.ShipMethodID,
				Comment = "Testing Insert Order",
				DueDate = DateTime.Today.AddDays(2),
				OrderDate = DateTime.Today,
				PurchaseOrderNumber = "0303030303",
				CreditCardApprovalCode = "XYZ",
				SalesOrderNumber = "ABC123",
				ModifiedDate = DateTime.Now,
				rowguid = Guid.NewGuid()
			};

			// create line item 1
			order.OrderDetails.Add(new SalesOrderDetail {
				SalesOrder = order, // we don't know the SalesOrderID yet
				UnitPrice = 1000.00M,
				OrderQty = 1,
				ProductID = specialOffers.ElementAt(0).ProductID,
				SpecialOfferID = specialOffers.ElementAt(0).SpecialOfferID,
				CarrierTrackingNumber = "09832-198230019823",
				ModifiedDate = DateTime.Now,
				rowguid = Guid.NewGuid()
			});

			// create line item 2
			order.OrderDetails.Add(new SalesOrderDetail {
				SalesOrder = order, // we don't know the SalesOrderID yet
				UnitPrice = 2000.00M,
				OrderQty = 1,
				ProductID = specialOffers.ElementAt(1).ProductID,
				SpecialOfferID = specialOffers.ElementAt(1).SpecialOfferID,
				CarrierTrackingNumber = "09832-198230019823",
				ModifiedDate = DateTime.Now,
				rowguid = Guid.NewGuid()
			});

			return order;
		}

		static string GetLastModifiedCurrencyBefore(DateTime givenDate) {
			// The first column of the first row in the resultset as T. Extra columns or rows are ignored.
			var code = c.ExecuteScalar<string>(new Script("SELECT TOP 1 [CurrencyCode] FROM [Sales].[Currency] WHERE [ModifiedDate] < @GivenDate ORDER BY [ModifiedDate] DESC", new { GivenDate = givenDate }));
			Console.WriteLine(string.Format("Last currency modified before {0} is {1}.", givenDate, code));
            return code;
		}

		static IEnumerable<dynamic> DynamicReadCurrencies(string nameLikeThis) {
			var s = @"
SELECT
	  [CurrencyCode]
	, [Name]
	, [ModifiedDate]
	, newid() as AnythingElse
FROM
	[Sales].[Currency]
where
	[Name] like @LikeThis";
                          
			// read dynamic objects
            IEnumerable<dynamic> currencies = c.ExecuteReader<dynamic>(s, new { LikeThis = nameLikeThis }, DynamicReader.Materialize);
			currencies.All(currency => {
				Console.WriteLine(string.Format("Currency: [{0}] {1} modified on {2} with {3}",
					currency.CurrencyCode, currency.Name, currency.ModifiedDate, currency.AnythingElse));
				return true;
			});

            return currencies;
		}

        static void InsertSalesOrder(IConnection cn, SalesOrderHeader order) {
            using (var ts = new TransactionScope()) {
                 // insert order (new order id is updated)
                cn.MapExecute(new SalesOrderHeaderRepository.SalesOrderHeaderInsert(), order);
                // insert details (order id is mapped from order)
                foreach (var od in order.OrderDetails)
                    cn.MapExecute(new SalesOrderDetailRepository.SalesOrderDetailInsert(), od);

                ts.Complete();
            }
            Console.WriteLine(string.Format("Sales order inserted. SalesOrderId={0}, Line1Id= {1}, Line2Id={2}",
                order.SalesOrderID, order.OrderDetails[0].SalesOrderDetailID, order.OrderDetails[1].SalesOrderDetailID));
        }

        static void DeleteSalesOrder(IConnection cn, SalesOrderHeader order) {
            using (var ts = new TransactionScope()) {
                cn.Execute(@"
delete from Sales.SalesOrderHeader where SalesOrderId = @orderid;
delete from Sales.SalesOrderDetail where SalesOrderId = @orderid;", new { orderid = order.SalesOrderID });
                ts.Complete();
            }
            Console.WriteLine(string.Format("Sales order delete in thread {1}. SalesOrderId={0}",
                order.SalesOrderID, System.Threading.Thread.CurrentThread.ManagedThreadId));
        }

		#endregion "Helpers"
	}
}