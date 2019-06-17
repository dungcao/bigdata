class Customers(name: String, debts: List[Int], credit: Int) {
	def totalDebt: Int = {
		debts.foldLeft()((x,y) => x+y)
	}
}

var debts = List(100,200,300)
var cus = new Customers("Dung Cao", debts, 1500)
println(cus.totalDebt)