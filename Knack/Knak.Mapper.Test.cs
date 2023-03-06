using Knak;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

/*
 * Unit testing (Tutorial)
 */
namespace Knak.Test {
	#region Test Model

	public class State {
		public int StateId { get; set; }
		public string Name { get; set; }
		public IEnumerable<City> Cities { get; set; }
		public DateTime TimeZone { get; set; }
	}

	public class City {
		public int CityId { get; set; }
		public string Name { get; set; }
		public State State { get; set; }
		public IEnumerable<Zip> ZipCodes { get; set; }
        public int Age { get; set; }
	}

	public class Zip {
		public int ZipId { get; set; }
		public string Number { get; set; }
		public City City { get; set; }
	}

	public class Address {
		public int AddressId { get; set; }
		public string Street1 { get; set; }
		public string Street2 { get; set; }
		public Zip Zip { get; set; }
	}

	public class Phone {
		public int PhoneId { get; set; }
		public int? AreaCode { get; set; }
		public int Number { get; set; }
	}

	public struct Pets {
		public int Cats { get; set; }
		public int Dogs { get; set; }
		public string Other { get; set; }
	}

 	public class Person {
		[Input]
		public int PersonId { get; set; }
		public string FirstName { get; set; }
		public string LastName { get; set; }
		public Address HomeAddress { get; set; }
		public Address WorkAddress { get; set; }
		public Phone HomePhone { get; set; }
		public Phone WorkPhone { get; set; }
		public DateTime WorkStartTime { get; set; }
		public Pets Pets { get; set; }
        public int Age { get; set; }

		static public Person CreateSample1() {
			return new Person {
				PersonId = 1,
				FirstName = "John",
				LastName = "Doe",
				Age = 30,
				HomeAddress = null,
				WorkAddress = new Address {
					AddressId = 1,
					Street1 = "123 .NET Drive",
					Street2 = "Apt 22",
					Zip = new Zip {
						City = new City {
							CityId = 1,
							Name = "Miami",
							State = new State { StateId = 1, Name = "FL" }
						}
					}
				},
				WorkPhone = new Phone { AreaCode = 305, Number = 2223333, PhoneId = 1 },
				WorkStartTime = new DateTime(2012, 10, 10, 9, 30, 00)
			};
		}

		static public Person CreateSample2() {
			return new Person {
				PersonId = 1,
				FirstName = "John",
				LastName = "Doe",
				Age = 30,
				HomePhone = new Phone { AreaCode = 305, Number = 2223333, PhoneId = 1 },
				WorkPhone = new Phone { Number = 1113333, PhoneId = 2 },
				HomeAddress = new Address {
					Zip = new Zip { City = new City { State = new State { TimeZone = new DateTime(2012, 10, 10, 3, 0, 0) } } }
				},
				Pets = new Pets { Cats = 2, Dogs = 4, Other = "Goat, Fish" }
			};
		}
	}

	public class WorkInfo {
		public string FirstName { get; set; }
		public string LastName { get; set; }
		public string Street1 { get; set; }
		public string Street2 { get; set; }
		public string City { get; set; }
		public int PhoneNumber { get; set; }
		public int StartHour { get; set; }
	}

	public class PersonInfo {
		public string FirstName { get; set; }
		[Output]
		public string LastName { get; set; }
		public int Age { get; set; }
		public int ZipId { get; set; }
		[Ignore]
		public int CityId { get; set; }
		public long StateId { get; set; }
		public long PersonId { get; set; }
		public long HomeAreaCode { get; set; }
		public long? WorkAreaCode { get; set; }
		public int StateZoneHour { get; set; }
		public Phone HomePhone { get; set; }
		public Pets Pets { get; set; }
	}

    public class OtherInfo {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public Phone[] Phones { get; set; }
        public Pets[] PetsInfo { get; set; }
    }

	#endregion Test Model

	public static class Logger {
		private static PropertyInfo debugView;

        public static void Log(IEnumerable<IMapping> mappings, LambdaExpression mapperLambda) {
			if(debugView == null)
				debugView = typeof(Expression).GetProperty("DebugView", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

			Console.WriteLine();
			Console.WriteLine("---------------- Mappings ---------------------");
			foreach (var m in mappings)
				Console.WriteLine(string.Format("{0} : {1} -> {2} : {3}", m.SourceType, m.Source, m.TargetType, m.Target));
			Console.WriteLine("---------------- Mappings ---------------------");

            if (mapperLambda != null) {
                Console.WriteLine();
                Console.WriteLine("---------------- Mapper Code ---------------------");
                Console.WriteLine((string)debugView.GetValue(mapperLambda, null));
                Console.WriteLine("---------------- Mapper Code ---------------------");
                Console.WriteLine();
            }
		}
	}

	[TestClass]
	public class TestMappings {
		[TestMethod]
		public void TestDefaultMappings() {
			Mapper.Of<Person, WorkInfo>.Binder.Reset();
			Logger.Log(Mapper.Of<Person, WorkInfo>.Mappings, Mapper.Of<Person, WorkInfo>.Expression);
            Assert.IsTrue(Mapper.Of<Person, WorkInfo>.Mappings.Count() == 4, "Expected 4 mappings");
		}

		[TestMethod]
		public void TestIgnoreInCustomMappings() {
            Mapper.Of<Person, WorkInfo>.Binder.Reset().Ignore(t => t.FirstName);
			Logger.Log(Mapper.Of<Person, WorkInfo>.Mappings, Mapper.Of<Person, WorkInfo>.Expression);
            Assert.IsTrue(Mapper.Of<Person, WorkInfo>.Mappings.Count() == 3, "Expected 3 mappings");
		}

		[TestMethod]
		public void TestCustomMapper() {
            Mapper.Of<Person, WorkInfo>.Binder.Reset()
				.Bind(s => s.WorkAddress.Street1, t => t.Street1)
				.Bind(s => s.WorkAddress.Street2, t => t.Street2)
				.Bind(s => s.WorkAddress.Zip.City.Name, t => t.City)
				.Bind(s => s.WorkPhone.Number, t => t.PhoneNumber)
				.Bind(s => s.WorkStartTime.Hour, t => t.StartHour);
			Logger.Log(Mapper.Of<Person, WorkInfo>.Mappings, Mapper.Of<Person, WorkInfo>.Expression);

			var person = Person.CreateSample1();
			var workinfo = new WorkInfo().MapFrom(person);

			Assert.AreEqual("John", workinfo.FirstName);
			Assert.AreEqual("Doe", workinfo.LastName);
			Assert.AreEqual("123 .NET Drive", workinfo.Street1);
			Assert.AreEqual("Apt 22", workinfo.Street2);
			Assert.AreEqual("Miami", workinfo.City);
			Assert.AreEqual(2223333, workinfo.PhoneNumber);
			Assert.AreEqual(9, workinfo.StartHour);
		}

		[TestMethod]
		public void TestCustomMapperWithCreateAndNullables() {
            Mapper.Of<Person, PersonInfo>.Binder.Reset()
				.Bind(s => s.HomePhone.AreaCode.HasValue ? s.HomePhone.AreaCode.Value : 0, t => t.HomeAreaCode)
				.Bind(s => s.WorkPhone.AreaCode, t => t.WorkAreaCode)
				.Bind(s => s.HomeAddress.Zip.City.State.TimeZone.Hour, t => t.StateZoneHour);
			Logger.Log(Mapper.Of<Person, PersonInfo>.Mappings, Mapper.Of<Person, PersonInfo>.Expression);

			var person = Person.CreateSample2();
			PersonInfo pinfo = person.MapTo(new PersonInfo());

			Assert.AreEqual("John", pinfo.FirstName);
			Assert.AreEqual(30, pinfo.Age);
			Assert.AreEqual(0, pinfo.ZipId);
			Assert.AreEqual(0, pinfo.StateId);
			Assert.AreEqual(305, pinfo.HomeAreaCode);
			Assert.AreEqual(3, pinfo.StateZoneHour);
			Assert.IsNull(pinfo.WorkAreaCode);
		}

		[TestMethod]
		public void TestDefaultMapperWithClassesAndStructs() {
			Mapper.Of<Person, PersonInfo>.Binder.Reset();
			Logger.Log(Mapper.Of<Person, PersonInfo>.Mappings, Mapper.Of<Person, PersonInfo>.Expression);
			Assert.IsTrue(Mapper.Of<Person, PersonInfo>.Mappings.Count() == 6, "Expected 6 mappings");

			var person = Person.CreateSample2();
			PersonInfo pinfo = person.MapTo(new PersonInfo());

			Assert.AreEqual("John", pinfo.FirstName);
			Assert.AreEqual(30, pinfo.Age);
			Assert.AreEqual(0, pinfo.ZipId);
			Assert.AreEqual(0, pinfo.StateId);
			Assert.IsNull(pinfo.WorkAreaCode);
			Assert.AreEqual(305, pinfo.HomePhone.AreaCode);
			Assert.AreEqual(2223333, pinfo.HomePhone.Number);
			Assert.AreEqual(1, pinfo.HomePhone.PhoneId);
			Assert.AreEqual(2, pinfo.Pets.Cats);
			Assert.AreEqual(4, pinfo.Pets.Dogs);
			Assert.AreEqual("Goat, Fish", pinfo.Pets.Other);
		}
	}

	[TestClass]
	public class TestMapper {
		[TestInitialize]
		public void Setup() {
            Mapper.Of<Person, WorkInfo>.Binder.Reset()
				.Bind(s => s.WorkAddress.Street1, t => t.Street1)
				.Bind(s => s.WorkAddress.Street2, t => t.Street2)
				.Bind(s => s.WorkAddress.Zip.City.Name, t => t.City)
				.Bind(s => s.WorkPhone.Number, t => t.PhoneNumber)
				.Bind(s => s.WorkStartTime.Hour, t => t.StartHour);
		}

		[TestMethod]
		public void TestMapperCustom() {
			var person = Person.CreateSample1();
			var workinfo = new WorkInfo().MapFrom(person);

			Assert.AreEqual("John", workinfo.FirstName);
			Assert.AreEqual("Doe", workinfo.LastName);
			Assert.AreEqual("123 .NET Drive", workinfo.Street1);
			Assert.AreEqual("Apt 22", workinfo.Street2);
			Assert.AreEqual("Miami", workinfo.City);
			Assert.AreEqual(2223333, workinfo.PhoneNumber);
			Assert.AreEqual(9, workinfo.StartHour);
		}

		[TestMethod]
		public void TestMapperDefault() {
			var person = Person.CreateSample2();
			var pinfo = new PersonInfo().MapFrom(person);

			Assert.AreEqual("John", pinfo.FirstName);
			Assert.AreEqual(30, pinfo.Age);
			Assert.AreEqual(0, pinfo.ZipId);
			Assert.AreEqual(0, pinfo.StateId);
		}

		[TestMethod]
		public void TestMapperDefaultCopy() {
			Logger.Log(Mapper.Of<Person, Person>.Mappings, Mapper.Of<Person, Person>.Expression);

			var person = Person.CreateSample2();
			person.HomeAddress.Zip.City.Name = "Miami";

			var person2 = new Person().MapFrom(person);

			// modify person now
			person.FirstName = "John changed";
			person.LastName = "Doe change";
			person.Age = 35;
			person.HomePhone.AreaCode = 505;
			person.HomePhone.Number = 444;
			person.HomeAddress.Zip.City.Name = "changed";
			person.Pets = new Pets();

			Assert.AreEqual("John", person2.FirstName);
			Assert.AreEqual("Doe", person2.LastName);
			Assert.AreEqual(30, person2.Age);
			Assert.AreEqual(305, person2.HomePhone.AreaCode);
			Assert.AreEqual(2223333, person2.HomePhone.Number);
			Assert.AreEqual("Miami", person2.HomeAddress.Zip.City.Name);
			Assert.AreEqual(2, person2.Pets.Cats);
		}

		[TestMethod]
		public void TestMapperDefaultCopyArray() {
			Person[] persons = new Person[3];
			persons[0] = Person.CreateSample1();
			persons[1] = Person.CreateSample2();
			persons[2] = null;

			Person[] persons2 = persons.MapArray<Person, Person>();

			Assert.AreEqual("John", persons2[0].FirstName);
			Assert.AreEqual("John", persons2[1].FirstName);
			Assert.IsNull(persons2[2]);

			Pets[] pets = new Pets[3];
			pets[0] = new Pets { Cats = 1, Dogs = 1, Other = "o1" };
			pets[1] = new Pets { Cats = 2, Dogs = 3, Other = "o2" };
			pets[2] = default(Pets);

            Pets[] petscopy = pets.Copy<Pets, Pets>();

			Assert.AreEqual(1, petscopy[0].Cats);
			Assert.AreEqual(2, petscopy[1].Cats);
			Assert.AreEqual(0, petscopy[2].Cats);

            int[] ints = new int[3];
            ints[0] = 0;
            ints[1] = 1;
            ints[2] = 2;

            long[] longs = ints.Copy<int, long>();
			Assert.AreEqual(0, longs[0]);
			Assert.AreEqual(1, longs[1]);
			Assert.AreEqual(2, longs[2]);
		}

        [TestMethod]
        public void TestMapperDefaultCopyArrays() {
			Logger.Log(Mapper.Of<OtherInfo, OtherInfo>.Mappings, Mapper.Of<OtherInfo, OtherInfo>.Expression);

            var oi1 = new OtherInfo {
                FirstName = "Other",
                LastName = "Info",
                Phones = new [] { new Phone { AreaCode = 111, Number = 222 }, new Phone { AreaCode = 333, Number = 444 } },
                PetsInfo = new [] { new Pets { Cats = 1 }, new Pets { Dogs = 2 }, new Pets { Other = "Birds" } }
            };

            var oi2 = oi1.MapTo(new OtherInfo());

            Assert.AreEqual(111, oi2.Phones[0].AreaCode);
            Assert.AreEqual(222, oi2.Phones[0].Number);
            Assert.AreEqual(333, oi2.Phones[1].AreaCode);
            Assert.AreEqual(444, oi2.Phones[1].Number);
            Assert.AreEqual(1, oi2.PetsInfo[0].Cats);
            Assert.AreEqual(2, oi2.PetsInfo[1].Dogs);
            Assert.AreEqual("Birds", oi2.PetsInfo[2].Other);
        }
	}
}
