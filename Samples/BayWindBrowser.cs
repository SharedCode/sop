using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;

namespace Sop.Samples
{
	public partial class BayWindBrowser : Form
	{
		public BayWindBrowser()
		{
			InitializeComponent();
		}

		private void buttonInitDB_Click(object sender, EventArgs e)
		{
			dataGridViewPeople.DataSource = null;
			dataGridViewPeople.Refresh();
			if (BayWind != null)
				BayWind.Dispose();
			if (System.IO.File.Exists(Sop.Samples.BayWind.ServerFilename))
			{
				//** delete entire directory to ensure all files including transaction 
				//** temp/logs and settings are removed as well...
				System.IO.Directory.Delete(Sop.Samples.BayWind.ServerPath, true);
				System.IO.Directory.CreateDirectory(Sop.Samples.BayWind.ServerPath);
			}
			BayWind = new BayWind();
			BayWind.Run();
            BayWind.Server.BeginTransaction();
			LoadData();
		}
		void LoadData()
		{
			if (BayWind == null)
				BayWind = new BayWind();

            //** Create a Dictionary Factory
            Sop.StoreFactory StoreFactory = new Sop.StoreFactory();

			//** retrieve the Object Stores from BayWind DB...
			PeopleStore =
				StoreFactory.Get<BayWind.PersonKey, BayWind.Person>(BayWind.Server.SystemFile.Store, "People", new BayWind.PersonComparer());
			AddressStore =
                StoreFactory.Get<int, BayWind.Address>(BayWind.Server.SystemFile.Store, "Address");
			PeopleStore.MoveFirst();
			DisplayPage();
		}

		private void BayWindBrowser_Load(object sender, EventArgs e)
		{
			textBoxBayWindDBFilename.Text = BayWind.ServerFilename;
			if (System.IO.File.Exists(Sop.Samples.BayWind.ServerFilename))
				LoadData();
			if (PeopleStore == null || PeopleStore.Count == 0)
				buttonInitDB_Click(sender, e);
		}

		private void buttonDelete_Click(object sender, EventArgs e)
		{
			BayWind.Person p = GetSelectedPerson();
			if (p != null)
			{
				PeopleStore.Remove(p.GetKey());
				GoPreviousPage();
				DisplayPage();
			}
		}

		BayWind.Person GetSelectedPerson()
		{
			if (dataGridViewPeople.SelectedRows.Count > 0)
				return (BayWind.Person)dataGridViewPeople.SelectedRows[0].DataBoundItem;
			return null;
		}

		private void buttonSearch_Click(object sender, EventArgs e)
		{
			if (!string.IsNullOrEmpty(textBoxFirstName.Text))
			{
				PeopleStore.Search(new BayWind.PersonKey() { FirstName = textBoxFirstName.Text, LastName = textBoxLastName.Text });
				DisplayPage();
			}
		}

		private void buttonMoveNextPage_Click(object sender, EventArgs e)
		{
			if (!PeopleStore.EndOfTree())
				DisplayPage();
		}

		private void buttonMoveLastPage_Click(object sender, EventArgs e)
		{
			PeopleStore.MoveLast();
			GoPreviousPage(1);
			PeopleStore.MoveNext();
			DisplayPage();
		}

		private void buttonFirstPage_Click(object sender, EventArgs e)
		{
			PeopleStore.MoveFirst();
			DisplayPage();
		}

		void GoPreviousPage()
		{
			GoPreviousPage(1);
		}
		void GoPreviousPage(int PageCount)
		{
			for (int ctr = 0; !PeopleStore.EndOfTree() && ctr < PageCount; ctr++)
			{
				int i = 0;
				while (i < PageSize &&
					!PeopleStore.EndOfTree())
				{
					i++;
					if (!PeopleStore.MovePrevious())
						break;
				}
			}
			if (PeopleStore.EndOfTree())
				PeopleStore.MoveFirst();
		}
		private void buttonMovePreviousPage_Click(object sender, EventArgs e)
		{
			GoPreviousPage(2);
			DisplayPage();
		}
		void DisplayPage()
		{
			BayWind.Person[] PeopleInPage = new BayWind.Person[PageSize];
			int i = 0;
			while (i < PeopleInPage.Length &&
				!PeopleStore.EndOfTree())
			{
				PeopleInPage[i++] = PeopleStore.CurrentValue;
				if (!PeopleStore.MoveNext())
					break;
			}
			if (PeopleStore.EndOfTree())
				PeopleStore.MoveLast();
			dataGridViewPeople.AutoGenerateColumns = true;
			dataGridViewPeople.DataSource = PeopleInPage;
			textBoxCount.Text = PeopleStore.Count.ToString();
		}

		private void dataGridViewPeople_SelectionChanged(object sender, EventArgs e)
		{
			BayWind.Person p = GetSelectedPerson();
			if (p != null)
				DisplayFields(p);
		}
		private void buttonQuit_Click(object sender, EventArgs e)
		{
			Close();
		}

		void DisplayFields(BayWind.Person p)
		{
			textBoxFirstName.Text = p.FirstName;
			textBoxLastName.Text = p.LastName;
			textBoxPhone.Text = p.PhoneNumber;
			if (AddressStore.Search(p.AddressID))
			{
				BayWind.Address addr = AddressStore.CurrentValue;
				textBoxStreet.Text = addr.Street;
				textBoxCity.Text = addr.City;
				textBoxState.Text = addr.State;
				textBoxCountry.Text = addr.Country;
				textBoxZip.Text = addr.ZipCode;
			}
		}
		private void buttonCommit_Click(object sender, EventArgs e)
		{
			if (PeopleStore.Transaction != null)
			{
				PeopleStore.Transaction.Commit();
                BayWind.Server.BeginTransaction();
			}
		}
		private void buttonAdd_Click(object sender, EventArgs e)
		{
			string Msg;
			if (!Validate(out Msg))
			{
				MessageBox.Show(Msg);
				return;
			}
			BayWind.Address addr = new BayWind.Address()
			{
				Street = textBoxStreet.Text,
				City = textBoxCity.Text,
				State = textBoxState.Text,
				Country = textBoxCountry.Text,
				ZipCode = textBoxZip.Text,
				AddressID = (int)AddressStore.GetNextSequence()
			};
			BayWind.Person p = new BayWind.Person()
			{
				FirstName = textBoxFirstName.Text,
				LastName = textBoxLastName.Text,
				PhoneNumber = textBoxPhone.Text,
				AddressID = addr.AddressID
			};
			PeopleStore.Add(p.GetKey(), p);
			AddressStore.Add(addr.AddressID, addr);
			GoPreviousPage();
			DisplayPage();
		}
		private void buttonUpdate_Click(object sender, EventArgs e)
		{
			string Msg;
			if (!Validate(out Msg))
			{
				MessageBox.Show(Msg);
				return;
			}
			BayWind.Address addr = new BayWind.Address()
			{
				Street = textBoxStreet.Text,
				City = textBoxCity.Text,
				State = textBoxState.Text,
				Country = textBoxCountry.Text,
				ZipCode = textBoxZip.Text,
				AddressID = (int)AddressStore.GetNextSequence()
			};
			BayWind.Person p = new BayWind.Person()
			{
				FirstName = textBoxFirstName.Text,
				LastName = textBoxLastName.Text,
				PhoneNumber = textBoxPhone.Text,
				AddressID = addr.AddressID
			};
			BayWind.Person p2 = GetSelectedPerson();
			if (p2 != null)
			{
				AddressStore.Remove(p2.AddressID);
				PeopleStore.Remove(p2.GetKey());
			}
			PeopleStore.Add(p.GetKey(), p);
			AddressStore.Add(addr.AddressID, addr);
			GoPreviousPage();
			DisplayPage();
		}
		bool Validate(out string Msg)
		{
			Msg = null;
			if (string.IsNullOrEmpty(textBoxFirstName.Text.Trim()))
			{
				Msg = "First Name is empty.";
				return false;
			}
			if (string.IsNullOrEmpty(textBoxLastName.Text.Trim()))
			{
				Msg = "Last Name is empty.";
				return false;
			}
			if (string.IsNullOrEmpty(textBoxPhone.Text.Trim()))
			{
				Msg = "Phone is empty.";
				return false;
			}
			if (string.IsNullOrEmpty(textBoxStreet.Text.Trim()))
			{
				Msg = "Street is empty.";
				return false;
			}
			return true;
		}

		ISortedDictionary<BayWind.PersonKey, BayWind.Person> PeopleStore;
		ISortedDictionary<int, BayWind.Address> AddressStore;
		BayWind BayWind;
		int PageSize = 11;

	}
}
