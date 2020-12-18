case class Customer(name: String, debt: Int, credit_limit: Int, credit_remained: Int = 0)

object Hello{
  def main(args: Array[String]): Unit = {
    println("Hello World!")
    val credit = 1000
    if (credit>500){
      println("greater than 500")
    }else {
      println("Else")
    }

    val debts = List(100, 200, 300)
    var i = 0
    while (i < debts.length) {
      println(debts(i))
      i += 1
    }

    println("FOR")
//    val debts = List(100, 200, 300)
    for (debt <- debts) {
      println(debt)
    }

    println("CLASS")
    var customers = List(
      Customer("Ti", 1000, 10000),
      Customer("Teo", 500, 5000),
      Customer("To", 1000, 10000))
//    customers.+(Customer("Added Cus", 3000, 10000))
    for (c <- customers){
      println(c.name + " - " + c.debt)
    }

    for (c <- customers if c.name.contains("o")) println(credit)
//    println(l)

    val name = "Tia"
    val job = name match {
      case "Ti" => "Nong dan"
      case _ => "Khong phai nong dan"
    }

    println(job)

    val gido = 3
    val job1 = gido match {
      case 1 => "Case 1"
      case 2 => "Case 2"
      case 3 => "Case 3"
      case _ => "Other"
    }
     println(job1)

    for (customer <- customers) {
      customer.credit_limit match {
        case x if x >= 10000 => println(customer.name + " is a VIP")
        case x if x > 1000 => println(customer.name + " is a normal")
        case _ => println(customer.name + " is low spender!")
      }
    }

    var b2 = debts.map(e => e*2)
    println(b2)
    println(b2.reduce((x,y) => x +y))
    println(b2.foldLeft(0)(_ * _))
  }
}