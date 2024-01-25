function deleteTriage(triageId) {
    fetch("/delete-triage", {
      method: "POST",
      body: JSON.stringify({ triageId: triageId }),
    }).then((_res) => {
      window.location.href = "/";
    });
  }